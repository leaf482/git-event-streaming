COMPOSE ?= docker compose
COMPOSE_FILES ?= -f docker-compose.yml
PROD_COMPOSE_FILES ?= -f docker-compose.yml -f docker-compose.prod.yml

.PHONY: build up down restart logs ps test tidy frontend-install frontend-build compose-config replay observability-targets prod-config prod-up prod-down prod-restart prod-logs prod-ps backup-postgres

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
