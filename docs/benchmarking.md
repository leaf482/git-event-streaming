# Benchmarking And Reliability Testing

Phase 6 adds reproducible single-node benchmark and resilience tooling. The goal is to measure operating limits, not to hide the limits of a Compose-based deployment.

## Synthetic Throughput Benchmark

Generate normalized GitHub events directly into Redpanda:

```sh
make bench-small
make bench
```

Tunable values:

- `BENCH_EVENTS`: total synthetic events.
- `BENCH_RATE`: maximum publish rate in events/sec.
- `BENCH_REPOS`: number of synthetic repositories.
- `BENCH_BATCH_SIZE`: Kafka write batch size.
- `BENCH_PREFIX`: event ID prefix.

Example:

```sh
BENCH_EVENTS=5000 BENCH_RATE=250 BENCH_REPOS=250 make bench
```

Watch:

- `github_pulse_consumer_events_processed_total`
- `github_pulse_consumer_processing_latency_seconds`
- `github_pulse_consumer_persistence_queue_depth`
- `kafka_consumergroup_lag`

## API And SSE Load Testing

k6 scripts live under `ops/k6`.

```sh
make k6-api
make k6-sse
```

The SSE script uses `once=true` to validate stream handshakes and event framing without holding each k6 iteration open forever. The dashboard still uses the normal long-lived SSE endpoints.

Tunable values:

- `K6_VUS`: concurrent virtual users.
- `K6_DURATION`: test duration.

Examples:

```sh
K6_VUS=25 K6_DURATION=1m make k6-api
K6_VUS=20 K6_DURATION=30s make k6-sse
```

## Replay Benchmarking

Bounded replay avoids accidentally replaying an entire retained topic:

```sh
REPLAY_MAX_MESSAGES=1000 REPLAY_RATE_LIMIT=100 make replay-bounded
```

Use replay benchmarking to compare:

- replay events/sec
- live consumer events/sec
- consumer lag behavior during replay
- Redis and PostgreSQL write failure rates

Replay remains safe because Redis idempotency markers and PostgreSQL `ON CONFLICT` handling prevent duplicate delivery from corrupting analytics.

## Failure Recovery Drills

Use explicit restart drills:

```sh
make recovery-redis
make recovery-postgres
make recovery-redpanda
make recovery-consumer
make recovery-ingestor
```

After each drill:

```sh
docker compose ps
curl http://localhost/api/trending/repos
curl http://localhost/prometheus/-/ready
curl "http://localhost/prometheus/api/v1/query?query=up"
```

Expected behavior:

- Redis restart may temporarily interrupt consumer readiness; consumer should recover after Redis is healthy.
- PostgreSQL restart should not stop realtime Redis rankings; persistence failures should be visible.
- Redpanda restart pauses ingestion/consumption; services should recover once broker health returns.
- Consumer/ingestor restarts should preserve data through Redpanda offsets and durable storage.

## Single-Node Expectations

On a small EC2 instance, start conservative:

- 50-250 synthetic events/sec.
- 5-25 API virtual users.
- 5-20 SSE clients.
- one PostgreSQL persistence worker.

Increase gradually while watching CPU, memory, consumer lag, queue depth, Redis memory, and PostgreSQL write latency.

## Bottlenecks To Watch

- Redpanda disk I/O and broker CPU.
- Redis `noeviction` memory pressure.
- PostgreSQL write latency and connection limits.
- Consumer async persistence queue depth.
- Caddy and frontend CPU during many SSE clients.
- GitHub API rate limits if testing the real ingestor.
