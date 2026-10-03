# Local infra + API server helpers.
# Individual targets can be run alone; composition targets chain them.

COMPOSE ?= docker compose
UV ?= uv run
HOST ?= 127.0.0.1
PORT ?= 8000
RELOAD ?= 0
AGENT_PORT ?= 2024

.PHONY: help compose-up compose-down compose-ps compose-logs \
	preflight server worker agent-server up start down stop

help:
	@echo "Individual:"
	@echo "  make compose-up     Start docker compose services (detached)"
	@echo "  make compose-down   Stop and remove compose services"
	@echo "  make compose-ps     Show compose service status"
	@echo "  make compose-logs   Follow compose logs"
	@echo "  make preflight      Validate configured Mission Control installation"
	@echo "  make server         Start scoped Mission Control API (uvicorn)"
	@echo "  make worker         Start Temporal worker"
	@echo "  make agent-server   Start canonical bounded Agent Server"
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
	$(UV) python -m mission_control.bootstrap.preflight

server:
ifeq ($(RELOAD),1)
	$(UV) uvicorn mission_control.bootstrap.api:create_app --factory --host $(HOST) --port $(PORT) --reload
else
	$(UV) uvicorn mission_control.bootstrap.api:create_app --factory --host $(HOST) --port $(PORT)
endif

worker:
	$(UV) python -m mission_control.bootstrap.worker

agent-server:
	$(UV) langgraph dev --config agent_server/langgraph.json --host $(HOST) --port $(AGENT_PORT) --no-browser

# --- Composition ---

up start: compose-up preflight server

down stop: compose-down
