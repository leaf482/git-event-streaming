# AWS EC2 Deployment Guide

This guide deploys GitHub Pulse on one low-cost EC2 instance with Docker Compose. It intentionally avoids Kubernetes, EKS, MSK, ECS/Fargate, Terraform, and managed databases so the system remains easy for one developer to operate.

## Deployment Architecture

```text
GitHub Public Events API
        ↓
EC2 instance
  ├─ github-events-ingestor
  ├─ github-events-consumer
  ├─ Next.js operational dashboard
  ├─ Redpanda single-node Kafka-compatible broker
  ├─ Redis
  └─ PostgreSQL
        ↓
Trending repository API / operational dashboard
```

Redpanda replaces the local Apache Kafka container because it keeps Kafka protocol compatibility while reducing single-node operational overhead. The Go producer still uses `KAFKA_BROKERS` and publishes to `github.events.raw.v1`.

## Cost Estimate

Approximate monthly cost in `us-east-1`, before free tier or regional differences:

| Resource | Low option | Safer option |
| --- | ---: | ---: |
| EC2 `t3.small` | $15-18 | |
| EC2 `t3.medium` | | $30-35 |
| 30 GB gp3 EBS | $2.40-3 |
| Elastic IP while attached | usually $0 |
| Data transfer | usually low for this phase |

Expected total is about `$18-25/month` on `t3.small` or `$35-45/month` on `t3.medium`. Use `t3.medium` once more consumers, API services, or sustained analytics workloads are added.

## Operational Tradeoffs

- This is production-style, not highly available. One EC2 instance means one failure domain.
- Redpanda, Redis, and PostgreSQL persist data in Docker volumes backed by EBS, so container restarts preserve state.
- EC2 reboot recovery depends on Docker restart policies and the Docker daemon starting at boot.
- Backups are operator-managed. Use PostgreSQL dumps and EBS snapshots before risky changes.
- This setup is a practical stepping stone toward managed services or multi-node deployments later.

## AWS Setup

1. Create an EC2 instance:
   - AMI: Ubuntu Server 24.04 LTS or 22.04 LTS
   - Instance type: `t3.small` for Phase 1, `t3.medium` for headroom
   - Storage: 30 GB gp3 minimum
   - IAM role: none required for Phase 1

2. Security group inbound rules:
   - SSH `22` from your IP only
   - Ingestor health `8080` from your IP only, or from `0.0.0.0/0` only for temporary testing
   - Consumer API `8081` from your IP only, or from `0.0.0.0/0` only for temporary testing
   - Frontend dashboard `3000` from your IP only, or route through a reverse proxy
   - Do not expose PostgreSQL `5432`, Redis `6379`, Redpanda `9092`, or Redpanda admin `9644` publicly

3. Optional domain setup:
   - Point an `A` record to the EC2 public IP or Elastic IP
   - Put a reverse proxy with TLS in front of port `8080` in a later phase

## Install Docker

Run on the EC2 instance:

```sh
sudo apt-get update
sudo apt-get install -y ca-certificates curl git make
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
```

Log out and back in so your shell can use Docker without `sudo`.

## Deploy

```sh
git clone <repo-url> git-event-streaming
cd git-event-streaming
cp .env.production.example .env.production
```

Edit `.env.production`:

```sh
nano .env.production
```

Set at least:

- `POSTGRES_PASSWORD` to a strong value
- `GITHUB_TOKEN` if you want higher GitHub API limits
- `INGESTOR_HOST_BIND=127.0.0.1` if using a reverse proxy on the same host
- `CONSUMER_HOST_BIND=127.0.0.1` if using a reverse proxy on the same host
- `FRONTEND_HOST_BIND=127.0.0.1` if using a reverse proxy on the same host

Start services:

```sh
make prod-up
```

Check status:

```sh
make prod-ps
curl http://localhost:8080/live
curl http://localhost:8080/ready
curl http://localhost:8080/metrics
curl http://localhost:8081/live
curl http://localhost:8081/ready
curl http://localhost:8081/metrics
curl http://localhost:8081/api/trending/repos
curl http://localhost:8081/api/events/recent
curl http://localhost:3000
```

View logs:

```sh
make prod-logs
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production logs -f ingestor
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production logs -f consumer
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production logs -f frontend
```

Inspect topic metadata:

```sh
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production exec redpanda rpk topic describe github.events.raw.v1 -X brokers=redpanda:9092
```

## Updating

```sh
git pull
make prod-config
make prod-up
```

`prod-up` rebuilds the Go service image and recreates changed containers. Persistent Docker volumes are retained.

## Restart And Rollback

Restart all services:

```sh
make prod-restart
```

Restart only the ingestor:

```sh
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production restart ingestor
```

Restart only the consumer:

```sh
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production restart consumer
```

Restart only the frontend:

```sh
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production restart frontend
```

Rollback to the previous Git commit:

```sh
git log --oneline -5
git checkout <previous-commit>
make prod-up
```

Stop without deleting volumes:

```sh
make prod-down
```

Do not run `docker compose down -v` in production unless you intentionally want to delete Redpanda, Redis, and PostgreSQL data.

## Auto-Restart After Reboot

The Compose services use `restart: unless-stopped`, and Docker is enabled with systemd. After an EC2 reboot, Docker should restart the containers automatically.

Verify:

```sh
sudo reboot
docker ps
curl http://localhost:8080/ready
curl http://localhost:8081/ready
curl http://localhost:3000
```

## Backups

PostgreSQL logical backup:

```sh
make backup-postgres
```

Recommended baseline:

- Run PostgreSQL dumps before deployments that change storage behavior.
- Create EBS snapshots before major infrastructure changes.
- Copy backups off the instance with `scp` or S3 in a later phase.
- Treat Redpanda topic data as replayable ingestion data for now; durable analytics should eventually live in PostgreSQL.

## Server Hardening Checklist

- Restrict SSH to your IP.
- Disable password SSH login.
- Keep only ports `22` and optionally `8080` open in the security group.
- Keep `8081` restricted to trusted sources until a public API/reverse proxy policy is added.
- Keep `3000` restricted or place it behind a TLS reverse proxy for public access.
- Store `.env.production` only on the server and never commit it.
- Rotate `POSTGRES_PASSWORD` and GitHub tokens if exposed.
- Enable unattended security updates if acceptable for your maintenance style.
- Keep EBS snapshots before risky updates.

## Production Deployment Checklist

- `.env.production` exists and contains a strong `POSTGRES_PASSWORD`.
- `docker compose config` passes through `make prod-config`.
- `docker ps` shows healthy Redpanda, Redis, PostgreSQL, ingestor, consumer, and frontend containers.
- `/live`, `/ready`, `/metrics`, `/api/trending/repos`, `/api/events/recent`, and the dashboard respond locally.
- Security group does not expose Redpanda, Redis, or PostgreSQL.
- Container logs are bounded by Docker log rotation.
- Docker is enabled on boot.
- A backup or EBS snapshot exists before major changes.

## Troubleshooting

Check containers:

```sh
docker ps
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production ps
```

Check Redpanda:

```sh
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production exec redpanda rpk cluster health -X brokers=redpanda:9092
```

Check PostgreSQL:

```sh
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production exec postgres pg_isready -U github_pulse -d github_pulse
```

Check Redis:

```sh
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.production exec redis redis-cli ping
```

If ingestion logs show GitHub rate limits, set `GITHUB_TOKEN` in `.env.production` and restart the ingestor.
