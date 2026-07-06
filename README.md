# TaskFlow

A tiny team task board, deliberately built to exercise all three of LogMind's open-source resource providers at once:

- **Redis** — task storage
- **RabbitMQ** — a background "notification" worker
- **MinIO** — file attachments per task

## Why this app looks the way it does

Every endpoint that touches a resource checks it actually exists *before* doing anything else, and lets a missing resource raise an unhandled exception rather than catching it and returning something empty/graceful. That means breaking a resource causes the **very next request** to fail loudly with a real traceback — no need to generate a burst of traffic before a failure becomes visible.

- Redis has no real schema, so a missing namespace doesn't fail a command on its own — `_check_redis()` explicitly checks for a marker key and raises if it's gone.
- RabbitMQ and MinIO's own client calls (`queue_declare(passive=True)`, `head_bucket`) already raise immediately if the queue/bucket doesn't exist, so no extra check is needed there.

## Endpoints

- `POST /tasks` — `{"title": "..."}` → creates a task, publishes a notification event
- `GET /tasks` — list all tasks
- `DELETE /tasks/{id}` — delete a task
- `POST /tasks/{id}/attach` — upload a file attachment (multipart form, field `file`)
- `GET /tasks/{id}/attachments` — list a task's attachments
- `GET /health` — checks the Redis connection

## Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `REDIS_URL` | Redis connection string | `redis://localhost:6379/0` |
| `REDIS_NAMESPACE` | Key prefix (set by a platform that provisions a dedicated namespace) | none |
| `RABBITMQ_URL` | AMQP connection string | `amqp://guest:guest@localhost:5672/%2F` |
| `RABBITMQ_QUEUE_NAME` | Notification queue name | `taskflow-notifications` |
| `S3_ENDPOINT_URL` | S3-compatible endpoint (MinIO) | `http://localhost:9000` |
| `S3_BUCKET_NAME` | Attachments bucket | `taskflow-attachments` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | MinIO credentials | `minioadmin` / `minioadmin` |

## Run locally

```bash
pip install -r requirements.txt
export REDIS_URL=redis://localhost:6379/0
export RABBITMQ_URL=amqp://guest:guest@localhost:5672/%2F
export S3_ENDPOINT_URL=http://localhost:9000
uvicorn main:app --reload
```

## Run with Docker

```bash
docker build -t taskflow .
docker run -p 8000:8000 \
  -e REDIS_URL=redis://host.docker.internal:6379/0 \
  -e RABBITMQ_URL=amqp://guest:guest@host.docker.internal:5672/%2F \
  -e S3_ENDPOINT_URL=http://host.docker.internal:9000 \
  taskflow
```
