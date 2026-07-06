import json
import os
import threading
import time
import uuid
from datetime import datetime

import boto3
import pika
import redis
from botocore.config import Config
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
# When run on a platform that provisions a dedicated Redis namespace for this
# app, scope every key under it - so that platform's own namespace lifecycle
# actually governs this app's real data. Falls back to unprefixed when unset.
REDIS_NAMESPACE = os.environ.get("REDIS_NAMESPACE", "")

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
QUEUE_NAME = os.environ.get("RABBITMQ_QUEUE_NAME", "taskflow-notifications")

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "taskflow-attachments")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

app = FastAPI(title="TaskFlow", description="A tiny team task board backed by Redis, RabbitMQ, and MinIO.")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    config=Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 1}, s3={"addressing_style": "path"}),
)


def _key(suffix: str) -> str:
    return f"{REDIS_NAMESPACE}:{suffix}" if REDIS_NAMESPACE else suffix


def _check_redis():
    """Redis is schemaless - a broken namespace never fails a command on its
    own (HGETALL on a missing key just returns empty). This explicit check is
    what makes a broken Redis resource actually crash the very next request
    instead of silently returning empty results."""
    if not r.exists(_key("__meta__")):
        raise RuntimeError("Redis namespace is missing - the task cache appears to have been deleted.")


def _rabbitmq_channel():
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    channel = connection.channel()
    # passive=True: only checks the queue exists, never recreates it - so a
    # broken queue stays broken (and visibly raises here) until something
    # (a person, or the AI) actually recreates it.
    channel.queue_declare(queue=QUEUE_NAME, durable=True, passive=True)
    return connection, channel


def _check_bucket():
    s3.head_bucket(Bucket=BUCKET_NAME)


def _notify_worker():
    """Consumes task-created events and marks the task as notified. Runs for
    the app's whole lifetime; if the queue is missing, it just keeps retrying
    (never recreates it) so a broken queue stays visibly broken."""
    while True:
        try:
            connection, channel = _rabbitmq_channel()

            def on_message(ch, method, properties, body):
                event = json.loads(body)
                time.sleep(1)  # simulated notification work
                r.hset(_key(f"task:{event['task_id']}"), "notified", "1")
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)
            channel.start_consuming()
        except Exception as e:
            print(f"Notify worker connection lost, retrying in 3s: {e}")
            time.sleep(3)


@app.on_event("startup")
def startup():
    # Best-effort, once, at startup only - creates the namespace/queue/bucket
    # if this is a genuinely fresh deployment with nothing provisioned yet.
    # Deliberately NOT repeated later, so breaking a resource afterwards stays
    # broken instead of silently healing on the next request.
    try:
        if not r.exists(_key("__meta__")):
            r.hset(_key("__meta__"), mapping={"created": "1"})
    except Exception as e:
        print(f"Warning: could not initialize Redis namespace at startup: {e}")

    try:
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        connection.channel().queue_declare(queue=QUEUE_NAME, durable=True)
        connection.close()
    except Exception as e:
        print(f"Warning: could not initialize RabbitMQ queue at startup: {e}")

    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
    except Exception:
        try:
            s3.create_bucket(Bucket=BUCKET_NAME)
        except Exception as e:
            print(f"Warning: could not initialize MinIO bucket at startup: {e}")

    threading.Thread(target=_notify_worker, daemon=True).start()


@app.get("/", response_class=HTMLResponse)
def index():
    with open("static/index.html", encoding="utf-8") as f:
        return f.read()


@app.get("/health")
def health():
    r.ping()
    return {"status": "ok"}


class TaskRequest(BaseModel):
    title: str


@app.post("/tasks")
def create_task(req: TaskRequest):
    _check_redis()
    task_id = str(uuid.uuid4())[:8]
    r.hset(
        _key(f"task:{task_id}"),
        mapping={"id": task_id, "title": req.title, "created_at": datetime.utcnow().isoformat() + "Z", "notified": "0"},
    )
    r.rpush(_key("task_ids"), task_id)

    connection, channel = _rabbitmq_channel()
    channel.basic_publish(exchange="", routing_key=QUEUE_NAME, body=json.dumps({"task_id": task_id, "title": req.title}))
    connection.close()

    return {"id": task_id, "title": req.title}


@app.get("/tasks")
def list_tasks():
    _check_redis()
    ids = r.lrange(_key("task_ids"), 0, -1)
    tasks = [r.hgetall(_key(f"task:{tid}")) for tid in ids]
    return {"tasks": [t for t in tasks if t]}


@app.delete("/tasks/{task_id}")
def delete_task(task_id: str):
    _check_redis()
    r.delete(_key(f"task:{task_id}"))
    r.lrem(_key("task_ids"), 0, task_id)
    return {"status": "deleted"}


@app.post("/tasks/{task_id}/attach")
def attach_file(task_id: str, file: UploadFile = File(...)):
    _check_bucket()
    content = file.file.read()
    key = f"{task_id}/{file.filename}"
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=content, ContentType=file.content_type or "application/octet-stream")
    return {"status": "uploaded", "key": key}


@app.get("/tasks/{task_id}/attachments")
def list_attachments(task_id: str):
    _check_bucket()
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=f"{task_id}/").get("Contents", [])
    return {"attachments": [{"key": o["Key"], "size": o["Size"]} for o in objects]}


# Deliberately kept at module scope (not a local var freed when the request
# ends) so each allocation survives past its own request - simulating a real
# memory leak rather than a transient spike the garbage collector would clean
# up on its own. This exists purely to demo a hosting platform's proactive
# memory-spike detection: sustained usage climbing toward the container's
# --memory limit, well before the kernel OOM-kills the process.
_memory_ballast: list[bytes] = []


@app.post("/simulate-load")
def simulate_load(add_mb: int = 50):
    # os.urandom, not bytes(n) - an all-zero buffer maps to the kernel's shared
    # zero page and never actually commits real RSS, so `docker stats` wouldn't
    # move at all. Random bytes force genuine, distinct physical pages.
    _memory_ballast.append(os.urandom(add_mb * 1024 * 1024))
    total_mb = sum(len(b) for b in _memory_ballast) // (1024 * 1024)
    return {"status": "allocated", "added_mb": add_mb, "total_leaked_mb": total_mb}


@app.post("/simulate-load/reset")
def reset_simulated_load():
    _memory_ballast.clear()
    return {"status": "reset"}
