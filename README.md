# GitHub Pulse

GitHub Pulse is a production-style distributed event analytics platform for ingesting GitHub public events and processing them through backend infrastructure.

The current implementation covers the ingestion pipeline, Redis-backed realtime aggregation, durable PostgreSQL analytics persistence, and a minimal operational dashboard:

- Go ingestion service
- GitHub Public Events API polling
- normalized event schema
- Kafka-compatible publishing
- Go consumer service
- Redis idempotency and trending repository aggregation
- PostgreSQL historical analytics snapshots
- Prometheus metrics collection
- Grafana operational dashboards
- Caddy reverse proxy foundation
- basic trending repository API
- historical repository trend APIs
- Next.js operational dashboard
- SSE-powered trending and event stream views
- Docker Compose infrastructure for Redpanda, Redis, and PostgreSQL
- structured JSON logging
- environment-based configuration
- basic health endpoints

The frontend is an operational analytics dashboard, not a GitHub clone or social app.

## Architecture

```text
GitHub Public Events API
        ↓
github-events-ingestor → Redpanda topic: github.events.raw.v1
        ↓
github-events-consumer
   ├── Redis sorted sets
   └── PostgreSQL historical analytics
        ↓
Caddy reverse proxy
   ├── Next.js dashboard + SSE
   ├── Consumer API
   ├── Grafana
   └── Prometheus
```

Redis remains the realtime hot path. PostgreSQL stores durable event metadata, hourly repository score snapshots, and aggregation windows for historical APIs and replay/backfill recovery.

## Project Structure

```text
cmd/ingestor        Go service entrypoint
cmd/consumer        Kafka consumer, Redis aggregation, and read API
cmd/replay          replay/backfill foundation worker
frontend            Next.js operational dashboard
ops/caddy           Caddy reverse proxy configuration
ops/grafana         Grafana datasource and dashboard provisioning
ops/prometheus      Prometheus scrape configuration
internal/config     environment configuration
internal/consumer   consumer metrics, Redis store, and API server
internal/events     normalized event schema
internal/github     GitHub Public Events API client
internal/kafka      Kafka producer
internal/persistence PostgreSQL analytics persistence
internal/server     health endpoints
```

## Kafka-Compatible Topics

| Topic | Purpose |
| --- | --- |
| `github.events.raw.v1` | Normalized GitHub public events produced by the ingestion service |

## Event Schema

Events published to Kafka use the normalized schema in `internal/events`:

- `schema_version`
- `source`
- `event_id`
- `event_type`
- `actor_id`
- `actor_login`
- `repo_id`
- `repo_name`
- `repo_url`
- `public`
- `created_at`
- `ingested_at`
- `payload`

The original GitHub event payload is preserved as JSON so future consumers can add analytics without requiring the ingestion service to understand every event-specific payload shape.

## Redis Aggregation

The consumer uses Redis for low-latency realtime state:

- `github_pulse:events:processed:<event_id>`: idempotency marker with TTL.
- `github_pulse:trending:repos`: sorted set of repository trending scores.
- `github_pulse:repos:metadata`: hash with compact repository metadata.
- `github_pulse:trending:repos:window:<bucket>`: time-windowed sorted sets with TTL.
- `github_pulse:events:recent`: bounded list for the live dashboard event stream.

Weighted scores:

- `WatchEvent`: `+3`
- `ForkEvent`: `+2`
- `PullRequestEvent`: `+2`
- `PushEvent`: `+1`
- `IssuesEvent`: `+1`
- `CreateEvent`: `+1`

## PostgreSQL Analytics

The consumer persists historical analytics asynchronously after Redis processing:

- `processed_event_metadata`: event ID keyed processing history.
- `repository_score_snapshots`: per-repository hourly score snapshots.
- `aggregation_windows`: durable window catalog.
- `repository_hourly_summaries`: summary foundation for future retention jobs.

Historical APIs:

```sh
curl http://localhost:8081/api/history/trending/repos
curl http://localhost:8081/api/history/windows
curl http://localhost:8081/api/history/repos/owner/repo
```

Replay/backfill foundation:

```sh
make replay
```

See `docs/postgres-analytics.md` for schema, replay, retention, and recovery details.

## Observability

Phase 5 adds Prometheus, Grafana, Caddy, and infrastructure exporters:

- Prometheus scrapes ingestor, consumer, Redpanda, Kafka lag, Redis, PostgreSQL, and Caddy metrics.
- Grafana provisions eight operational dashboards from `ops/grafana/dashboards`.
- Caddy exposes the dashboard, APIs, Prometheus, and Grafana behind a single HTTP entrypoint.

Local URLs:

```sh
open http://localhost/
open http://localhost/grafana/
open http://localhost/prometheus/
```

See `docs/observability.md` for dashboard usage, scrape targets, reverse proxy behavior, and troubleshooting.

## Configuration

`.env.example` documents the supported environment variables. Docker Compose also reads a local `.env` file for variable interpolation, such as `GITHUB_TOKEN`.

Key environment variables:

- `GITHUB_TOKEN`: optional GitHub token for higher API limits.
- `POLL_INTERVAL`: interval between GitHub API polls, default `15s`.
- `KAFKA_BROKERS`: comma-separated Kafka broker list.
- `KAFKA_TOPIC_GITHUB_EVENTS`: Kafka topic for normalized events.
- `HTTP_ADDR`: health server bind address.
- `REDIS_ADDR`: Redis address used by the consumer.
- `POSTGRES_DSN`: PostgreSQL connection string used by the consumer.
- `POSTGRES_ENABLED`: enables historical persistence and APIs, default `true`.
- `PERSISTENCE_QUEUE_SIZE`: async PostgreSQL write queue size.
- `REPLAY_MODE`: marks replay runs in logs/metrics.
- `IDEMPOTENCY_TTL`: TTL for processed GitHub event IDs.
- `TRENDING_LIMIT`: maximum default repositories returned by the trending API.
- `PROMETHEUS_RETENTION`: local Prometheus data retention, default `15d`.
- `GRAFANA_ADMIN_PASSWORD`: Grafana admin password for production.
- `CADDY_HTTP_HOST_BIND`: Caddy HTTP bind address.

## Running Locally

Start the full stack:

```sh
docker compose up --build
```

Run only infrastructure:

```sh
docker compose up redpanda kafka-init redis postgres
```

Run the ingestor from the host after infrastructure is ready:

```powershell
$env:KAFKA_BROKERS = "localhost:9092"
go run ./cmd/ingestor
```

Health endpoints:

```sh
curl http://localhost:8080/live
curl http://localhost:8080/ready
curl http://localhost:8080/metrics
curl http://localhost:8081/live
curl http://localhost:8081/ready
curl http://localhost:8081/metrics
```

Dashboard:

```sh
open http://localhost/
open http://localhost:3000
open http://localhost/grafana/
open http://localhost/prometheus/
```

Trending repositories:

```sh
curl http://localhost:8081/api/trending/repos
curl "http://localhost:8081/api/trending/repos?limit=5"
curl http://localhost:8081/api/events/recent
curl http://localhost:8081/api/history/trending/repos
curl http://localhost:8081/api/history/windows
curl http://localhost/api/trending/repos
```

SSE streams:

```sh
curl -N http://localhost:8081/api/trending/repos/stream
curl -N http://localhost:8081/api/events/stream
curl -N http://localhost/api/trending/repos/stream
```

Run the frontend directly:

```sh
cd frontend
npm install
npm run dev
```

Inspect Kafka messages:

```sh
docker compose exec redpanda rpk topic consume github.events.raw.v1 -X brokers=redpanda:9092
```

Inspect Redis aggregation state:

```sh
docker compose exec redis redis-cli ZREVRANGE github_pulse:trending:repos 0 9 WITHSCORES
```

## AWS EC2 Deployment

This repository is optimized for low-cost single-node EC2 deployment with Docker Compose. See `docs/aws-ec2-deployment.md` for setup commands, security group guidance, cost estimates, backup recommendations, and operational runbooks.

