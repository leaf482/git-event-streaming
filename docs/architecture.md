# Architecture

GitHub Pulse is an event-driven analytics platform. It is intentionally infrastructure-focused: the value is in ingestion, streaming, aggregation, persistence, replayability, observability, and operational recovery.

## System Architecture

```mermaid
flowchart TB
    github[GitHub Public Events API]
    ingestor[Go Ingestor]
    redpanda[(Redpanda / Kafka API)]
    consumer[Go Consumer]
    redis[(Redis realtime state)]
    postgres[(PostgreSQL historical analytics)]
    caddy[Caddy reverse proxy]
    frontend[Next.js operations dashboard]
    grafana[Grafana dashboards]
    prometheus[(Prometheus)]

    github --> ingestor
    ingestor --> redpanda
    redpanda --> consumer
    consumer --> redis
    consumer --> postgres
    redis --> consumer
    postgres --> consumer
    consumer --> caddy
    frontend --> caddy
    caddy --> frontend
    caddy --> grafana
    caddy --> prometheus
    prometheus --> grafana
```

## Event Flow

```mermaid
sequenceDiagram
    participant GH as GitHub Events API
    participant I as Ingestor
    participant K as Redpanda Topic
    participant C as Consumer
    participant R as Redis
    participant P as PostgreSQL
    participant API as API/SSE

    I->>GH: poll public events
    GH-->>I: event batch
    I->>I: normalize schema
    I->>K: publish github.events.raw.v1
    C->>K: consume with group offsets
    C->>R: idempotency + realtime ranking
    C->>P: async durable analytics write
    API->>R: live trending + recent events
    API->>P: historical trends + windows
```

## Replay And Backfill

```mermaid
flowchart LR
    topic[(Retained Redpanda topic)]
    replay[Replay worker]
    redis[(Redis rebuild)]
    postgres[(PostgreSQL upsert)]
    metrics[Replay metrics/logs]

    topic --> replay
    replay --> redis
    replay --> postgres
    replay --> metrics
```

Replay is intentionally bounded and operator-controlled. Redis idempotency markers and PostgreSQL conflict handling make duplicate delivery safe for analytics recovery.

## Observability

```mermaid
flowchart TB
    ingestor[Ingestor metrics]
    consumer[Consumer metrics]
    redpanda[Redpanda metrics]
    redis[Redis exporter]
    postgres[Postgres exporter]
    caddy[Caddy metrics]
    kafka[Kafka lag exporter]
    prometheus[(Prometheus)]
    grafana[Grafana]

    ingestor --> prometheus
    consumer --> prometheus
    redpanda --> prometheus
    redis --> prometheus
    postgres --> prometheus
    caddy --> prometheus
    kafka --> prometheus
    prometheus --> grafana
```

## Deployment Architecture

```mermaid
flowchart TB
    internet[Browser / Operator]
    ec2[Single EC2 instance]
    caddy[Caddy :80 / future :443]
    app[Go + Next.js services]
    data[(Docker volumes: Redpanda, Redis, Postgres, Prometheus, Grafana)]

    internet --> caddy
    caddy --> app
    app --> data
    ec2 --> caddy
    ec2 --> app
    ec2 --> data
```

This is production-style but not highly available. The single-node design keeps cost and operational complexity low while preserving real distributed systems concerns: queues, offsets, idempotency, backpressure, replay, and observability.
