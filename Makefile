COMPOSE ?= docker compose
COMPOSE_FILES ?= -f docker-compose.yml
PROD_COMPOSE_FILES ?= -f docker-compose.yml -f docker-compose.prod.yml

.PHONY: build up down restart logs ps test tidy frontend-install frontend-build compose-config bench bench-small replay replay-bounded k6-api k6-sse recovery-redis recovery-postgres recovery-redpanda recovery-consumer recovery-ingestor observability-targets prod-config prod-up prod-down prod-restart prod-logs prod-ps backup-postgres

build:
	$(COMPOSE) $(COMPOSE_FILES) build

up:
	$(COMPOSE) $(COMPOSE_FILES) up -d --build

down:
	$(COMPOSE) $(COMPOSE_FILES) down

restart:
	$(COMPOSE) $(COMPOSE_FILES) restart

logs:
	$(COMPOSE) $(COMPOSE_FILES) logs -f --tail=200

ps:
	$(COMPOSE) $(COMPOSE_FILES) ps

test:
	go test ./...

tidy:
	go mod tidy

frontend-install:
	cd frontend && npm install

frontend-build:
	cd frontend && npm run build

compose-config:
	$(COMPOSE) $(COMPOSE_FILES) config

replay:
	$(COMPOSE) $(COMPOSE_FILES) run --rm --entrypoint /app/replay consumer

replay-bounded:
	$(COMPOSE) $(COMPOSE_FILES) run --rm --entrypoint /app/replay -e REPLAY_MAX_MESSAGES=$${REPLAY_MAX_MESSAGES:-1000} -e REPLAY_RATE_LIMIT=$${REPLAY_RATE_LIMIT:-100} consumer

bench:
	$(COMPOSE) $(COMPOSE_FILES) run --rm --entrypoint /app/benchgen -e BENCH_EVENTS=$${BENCH_EVENTS:-1000} -e BENCH_RATE=$${BENCH_RATE:-100} -e BENCH_REPOS=$${BENCH_REPOS:-100} consumer

bench-small:
	$(COMPOSE) $(COMPOSE_FILES) run --rm --entrypoint /app/benchgen -e BENCH_EVENTS=100 -e BENCH_RATE=50 -e BENCH_REPOS=20 consumer

k6-api:
	$(COMPOSE) $(COMPOSE_FILES) run --rm -e BASE_URL=http://caddy -e K6_VUS=$${K6_VUS:-10} -e K6_DURATION=$${K6_DURATION:-30s} k6 run /scripts/api-latency.js

k6-sse:
	$(COMPOSE) $(COMPOSE_FILES) run --rm -e BASE_URL=http://caddy -e K6_VUS=$${K6_VUS:-5} -e K6_DURATION=$${K6_DURATION:-20s} k6 run /scripts/sse-smoke.js

recovery-redis:
	$(COMPOSE) $(COMPOSE_FILES) restart redis redis-exporter consumer && $(COMPOSE) $(COMPOSE_FILES) ps

recovery-postgres:
	$(COMPOSE) $(COMPOSE_FILES) restart postgres postgres-exporter consumer && $(COMPOSE) $(COMPOSE_FILES) ps

recovery-redpanda:
	$(COMPOSE) $(COMPOSE_FILES) restart redpanda kafka-exporter ingestor consumer && $(COMPOSE) $(COMPOSE_FILES) ps

recovery-consumer:
	$(COMPOSE) $(COMPOSE_FILES) restart consumer && $(COMPOSE) $(COMPOSE_FILES) ps

recovery-ingestor:
	$(COMPOSE) $(COMPOSE_FILES) restart ingestor && $(COMPOSE) $(COMPOSE_FILES) ps

observability-targets:
	$(COMPOSE) $(COMPOSE_FILES) exec -T prometheus wget -qO- http://127.0.0.1:9090/prometheus/api/v1/targets

prod-config:
	$(COMPOSE) $(PROD_COMPOSE_FILES) --env-file .env.production config

prod-up:
	$(COMPOSE) $(PROD_COMPOSE_FILES) --env-file .env.production up -d --build

prod-down:
	$(COMPOSE) $(PROD_COMPOSE_FILES) --env-file .env.production down

prod-restart:
	$(COMPOSE) $(PROD_COMPOSE_FILES) --env-file .env.production restart

prod-logs:
	$(COMPOSE) $(PROD_COMPOSE_FILES) --env-file .env.production logs -f --tail=200

prod-ps:
	$(COMPOSE) $(PROD_COMPOSE_FILES) --env-file .env.production ps

backup-postgres:
	mkdir -p backups
	$(COMPOSE) $(PROD_COMPOSE_FILES) --env-file .env.production exec -T postgres sh -c 'pg_dump -U "$$POSTGRES_USER" "$$POSTGRES_DB"' > backups/postgres-$$(date +%Y%m%d-%H%M%S).sql
