# Production Runbook

This runbook is the operational checklist for launching GitHub Pulse on a single EC2 instance.

## Recommended Instance

- AMI: Ubuntu Server 24.04 LTS
- Instance: `t3.medium`
- Storage: 50 GB gp3
- Inbound ports: `22` from your IP, `80` public or trusted IPs, `443` after TLS
- Keep `5432`, `6379`, `9092`, `9644`, `8080`, `8081`, `3000`, `3001`, and `9090` private

The verified deployment target used for launch prep was Ubuntu 24.04 with 2 vCPU, about 3.7 GiB RAM, and a 48 GiB root volume.

## Environment Checklist

Create `.env.production` from `.env.production.example` and set:

- `POSTGRES_PASSWORD`
- `POSTGRES_DSN`
- `GRAFANA_ADMIN_PASSWORD`
- `GITHUB_TOKEN` if you want higher GitHub API limits
- `INGESTOR_HOST_BIND=127.0.0.1`
- `CONSUMER_HOST_BIND=127.0.0.1`
- `FRONTEND_HOST_BIND=127.0.0.1`
- `PROMETHEUS_HOST_BIND=127.0.0.1`
- `GRAFANA_HOST_BIND=127.0.0.1`

Do not commit `.env.production`.

## Launch Checklist

```sh
make prod-config
make prod-up
make prod-ps
```

Validate:

```sh
curl http://localhost/
curl http://localhost/api/trending/repos
curl http://localhost/api/history/windows
curl http://localhost/prometheus/-/ready
curl http://localhost/grafana/api/health
curl -N --max-time 5 "http://localhost/api/events/stream?once=true"
```

Open:

- `http://<public-ip>/`
- `http://<public-ip>/grafana/`
- `http://<public-ip>/prometheus/`

## Backup Checklist

- Run `make backup-postgres` before risky changes.
- Snapshot the EC2 EBS volume before deployments that change storage behavior.
- Preserve Docker volumes for Redpanda, Redis, PostgreSQL, Prometheus, Grafana, and Caddy.
- Copy important backups off the instance in a future phase.

## Recovery Checklist

```sh
make recovery-consumer
make recovery-ingestor
make recovery-redis
make recovery-postgres
make recovery-redpanda
```

After recovery:

```sh
docker compose ps
curl http://localhost/api/trending/repos
curl http://localhost/prometheus/-/ready
curl "http://localhost/prometheus/api/v1/query?query=up"
```

If historical persistence has gaps and Redpanda still retains events:

```sh
REPLAY_MAX_MESSAGES=1000 REPLAY_RATE_LIMIT=100 make replay-bounded
```

## Public Demo Checklist

- Dashboard loads through Caddy.
- Trending repositories and recent events respond.
- Grafana dashboards are provisioned.
- Prometheus targets are healthy.
- README screenshots are updated or placeholder guidance is accurate.
- Security group exposes only intended ports.
- `.env.production` remains untracked.

## Known Limits

- Single EC2 instance means no high availability.
- Redpanda, Redis, PostgreSQL, Prometheus, and Grafana share CPU and memory.
- GitHub unauthenticated API calls can hit rate limits quickly.
- Prometheus history and Redpanda retention consume disk over time.
- This setup is designed for portfolio/demo and operational learning, not multi-region production traffic.
