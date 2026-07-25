# Local infra + API server helpers.
# Individual targets can be run alone; composition targets chain them.

COMPOSE ?= docker compose
UV ?= uv run
HOST ?= 127.0.0.1
PORT ?= 8000
RELOAD ?= 0

.PHONY: help compose-up compose-down compose-ps compose-logs \
	preflight server worker up start down stop

help:
	@echo "Individual:"
	@echo "  make compose-up     Start docker compose services (detached)"
	@echo "  make compose-down   Stop and remove compose services"
	@echo "  make compose-ps     Show compose service status"
	@echo "  make compose-logs   Follow compose logs"
	@echo "  make preflight      Run app.preflight checks"
	@echo "  make server         Start FastAPI/Socket.IO (uvicorn)"
	@echo "  make worker         Start Temporal worker"
	@echo ""
	@echo "Composition:"
	@echo "  make up / start     compose-up -> preflight -> server"
	@echo "  make down / stop    compose-down"
	@echo ""
	@echo "Overrides: HOST=$(HOST) PORT=$(PORT) RELOAD=$(RELOAD)"

# --- Individual: Docker Compose ---

compose-up:
	$(COMPOSE) up -d

compose-down:
	$(COMPOSE) down

compose-ps:
	$(COMPOSE) ps

compose-logs:
	$(COMPOSE) logs -f

# --- Individual: Application ---

preflight:
	$(UV) python -m app.preflight

server:
ifeq ($(RELOAD),1)
	$(UV) uvicorn app.server:asgi_app --host $(HOST) --port $(PORT) --reload
else
	$(UV) uvicorn app.server:asgi_app --host $(HOST) --port $(PORT)
endif

worker:
	$(UV) python -m app.temporal.worker

# --- Composition ---

up start: compose-up preflight server

down stop: compose-down
