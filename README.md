# Distributed Job Scheduler (Mini-Temporal)

This project is a production-grade, minimal distributed job scheduler inspired by Temporal/Airflow. It provides a scheduler, worker, and API service backed by PostgreSQL for durable state and Redis for queueing and locks. Internal communication uses gRPC; external clients use a REST API.

## Architecture

- **API service** (`api/`): REST endpoints for submitting DAG jobs and querying status.
- **Scheduler** (`scheduler/`): Persists DAGs, enqueues runnable nodes, and handles retries/timeouts.
- **Worker** (`worker/`): Polls Redis for runnable tasks and executes node commands.
- **PostgreSQL**: Durable job and node state.
- **Redis**: Task queue and retry coordination.

### Execution flow
1. API submits a DAG to the scheduler via gRPC.
2. Scheduler persists the DAG and enqueues runnable nodes into Redis.
3. Workers poll Redis, claim tasks from the scheduler, execute commands, and report results.
4. Scheduler updates node state, enqueues downstream nodes when dependencies succeed, and retries failures with exponential backoff.

## Running locally

```bash
docker compose up --build
```

The API will be available at `http://localhost:8080`.

## Example job submission

```bash
curl -X POST http://localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job": {
      "name": "example-dag",
      "nodes": [
        {
          "id": "extract",
          "name": "extract",
          "command": "echo extract",
          "retries": 2,
          "timeout_seconds": 10,
          "idempotency_key": "extract-v1",
          "depends_on": []
        },
        {
          "id": "transform",
          "name": "transform",
          "command": "echo transform",
          "retries": 2,
          "timeout_seconds": 10,
          "idempotency_key": "transform-v1",
          "depends_on": ["extract"]
        },
        {
          "id": "load",
          "name": "load",
          "command": "echo load",
          "retries": 2,
          "timeout_seconds": 10,
          "idempotency_key": "load-v1",
          "depends_on": ["transform"]
        }
      ]
    }
  }'
```

Get job status:

```bash
curl http://localhost:8080/v1/jobs/<job-id>
```

## Failure recovery

- **Scheduler restarts**: state is persisted in PostgreSQL; runnable nodes are re-enqueued from pending/retry status.
- **Worker crashes**: tasks stuck in `running` are detected by the scheduler and retried.
- **Retries**: failed nodes are marked `retry_wait` with exponential backoff and re-queued when due.

## Observability

Each service exposes:

- `GET /healthz`
- `GET /metrics` (Prometheus)

Scheduler metrics include task latency, retries, and failures.
