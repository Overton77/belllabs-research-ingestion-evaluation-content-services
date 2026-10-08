# Mission Control developer entrypoints.
#
# Every target is independent; composition targets at the bottom chain them.
# `make help` lists everything. Variables can be overridden per call, e.g.
#   make server PORT=8100 RELOAD=1
#   make infra-logs SERVICE=temporal
#   make test-unit PYTEST_ARGS="-k reducer -x"
#
# Recipes run under `sh` (Git for Windows provides one), so the same Makefile works from
# PowerShell, Git Bash, WSL and the Cursor cloud image.

.DEFAULT_GOAL := help
SHELL := sh

# --- Tooling ---------------------------------------------------------------------------
UV        ?= uv
RUN       ?= $(UV) run --no-sync
COMPOSE   ?= docker compose
COMPOSE_TEMPORAL_ONLY ?= docker compose -f docker-compose.temporal.yml

# --- Application processes --------------------------------------------------------------
HOST        ?= 127.0.0.1
PORT        ?= 8000
RELOAD      ?= 0
AGENT_PORT  ?= 2024
MCP_PORT    ?= 8010
MCP_PATH    ?= /mcp

# --- Infrastructure (host-side ports; see docker-compose.yml) ---------------------------
APPLICATION_POSTGRES_PORT ?= 55432
REDIS_PORT                ?= 16379
TEMPORAL_UI_PORT          ?= 8080
WAIT_TIMEOUT              ?= 300
SERVICE                   ?=

# --- Database component (mission-db) -----------------------------------------------------
APP            ?= biotech
PKG            := packages/mission-control-db-contract
DEPLOYMENT_DIR ?= deployments/$(APP)
SEED_BUNDLES   ?= --bundle $(PKG)/seeds/common --bundle $(PKG)/seeds/$(APP)
OUT_DIR        ?= .scratch/mission-db

# --- Tests -------------------------------------------------------------------------------
PYTEST_ARGS ?=
PYTEST      := $(RUN) pytest $(PYTEST_ARGS)

UVICORN_FLAGS := --factory --host $(HOST) --port $(PORT)
ifeq ($(RELOAD),1)
UVICORN_FLAGS += --reload --reload-dir src
endif

.PHONY: help \
	install install-all lock lock-check lock-upgrade outdated tree hooks doctor env-check \
	infra-up infra-down infra-restart infra-ps infra-logs infra-pull infra-config wait \
	db-up redis-up temporal-up temporal-only-up temporal-only-down temporal-ui temporal \
	psql psql-temporal redis-cli \
	preflight server worker agent-server mcp-server health openapi socketio-smoke missionctl \
	mission-db db-inspect db-plan db-verify db-runtime-plan db-seed-plan db-snapshot db-apply \
	fmt fmt-check lint lint-fix typecheck typecheck-fast typecheck-watch typecheck-daemon \
	deps-check audit links precommit arch skills-manifest skills-check seeds-validate check ci \
	test test-unit test-unit-fast test-arch test-integration test-acceptance test-qualification \
	test-db-contract test-failed coverage \
	clean up dev down stop status

# ========================================================================================
help: ## Show this help
	@$(RUN) python scripts/dev/make_help.py $(MAKEFILE_LIST)

# ========================================================================================
##@ Environment

install: ## Sync runtime + dev + dbcontract (mission-db) + biotech (needed by the unit suite)
	$(UV) sync --group dbcontract --group biotech

install-all: ## Sync every dependency group (biotech, dbcontract, notebooks)
	$(UV) sync --all-groups

lock: ## Re-resolve uv.lock without upgrading pinned versions
	$(UV) lock

lock-check: ## Fail if uv.lock is out of date with pyproject.toml
	$(UV) lock --check

lock-upgrade: ## Upgrade every dependency within its constraints, then sync
	$(UV) lock --upgrade
	$(UV) sync --all-groups

outdated: ## List installed packages with newer releases
	$(UV) pip list --outdated

tree: ## Show the resolved dependency tree (depth 1)
	$(UV) tree --depth 1

hooks: ## Install the git pre-commit hook (prek, a Rust pre-commit runner)
	$(RUN) prek install

doctor: ## Print tool versions and check the local toolchain
	@echo "uv:      $$($(UV) --version)"
	@echo "python:  $$($(RUN) python --version)"
	@echo "ruff:    $$($(RUN) ruff --version)"
	@echo "mypy:    $$($(RUN) mypy --version)"
	@echo "ty:      $$($(RUN) ty --version)"
	@echo "pytest:  $$($(RUN) pytest --version 2>&1 | tail -1)"
	@echo "make:    $$(make --version | head -1)"
	@echo "docker:  $$(docker --version)"
	@echo "compose: $$($(COMPOSE) version)"

env-check: ## Report .env names missing versus .env.example (names only; STRICT=1 to fail)
	$(RUN) python scripts/dev/env_check.py $(if $(STRICT),--strict,)

# ========================================================================================
##@ Infrastructure (Docker Compose: PostgreSQL + pgvector, Redis, Temporal, Temporal UI)

infra-up: ## Start all compose services detached and wait until healthy
	$(COMPOSE) up -d
	$(MAKE) wait

infra-down: ## Stop and remove compose containers (volumes are kept)
	$(COMPOSE) down

infra-restart: ## Restart compose services (or one with SERVICE=name)
	$(COMPOSE) restart $(SERVICE)

infra-ps: ## Show compose service status
	$(COMPOSE) ps --all

infra-logs: ## Follow compose logs (all, or SERVICE=name)
	$(COMPOSE) logs -f --tail=200 $(SERVICE)

infra-pull: ## Pull the pinned images
	$(COMPOSE) pull

infra-config: ## Validate the compose files
	$(COMPOSE) config --quiet
	$(COMPOSE_TEMPORAL_ONLY) config --quiet
	@echo "compose configuration is valid"

wait: ## Block until compose services are healthy and one-shot jobs have exited 0
	$(RUN) python scripts/dev/wait_for_services.py --timeout $(WAIT_TIMEOUT)

db-up: ## Start only the application PostgreSQL
	$(COMPOSE) up -d application-postgres

redis-up: ## Start only Redis
	$(COMPOSE) up -d redis

temporal-up: ## Start the Temporal stack (server, schema job, namespace job, UI)
	$(COMPOSE) up -d temporal temporal-create-namespace temporal-ui

temporal-only-up: ## Start the isolated Temporal-only stack (docker-compose.temporal.yml)
	$(COMPOSE_TEMPORAL_ONLY) up -d

temporal-only-down: ## Stop the isolated Temporal-only stack
	$(COMPOSE_TEMPORAL_ONLY) down

temporal-ui: ## Print the Temporal UI URL
	@echo "Temporal UI: http://127.0.0.1:$(TEMPORAL_UI_PORT)   gRPC: 127.0.0.1:7233"

temporal: ## Run the Temporal CLI inside the admin-tools container, e.g. make temporal ARGS="workflow list"
	$(COMPOSE) --profile tools run --rm temporal-admin-tools temporal $(ARGS)

psql: ## Open psql on the application database (belllabs)
	$(COMPOSE) exec application-postgres psql -U belllabs -d belllabs

psql-temporal: ## Open psql on the Temporal persistence database
	$(COMPOSE) exec temporal-postgres psql -U temporal

redis-cli: ## Open redis-cli on the compose Redis
	$(COMPOSE) exec redis redis-cli

# ========================================================================================
##@ Application processes

preflight: ## Read-only validation of the configured Mission Control installation
	$(RUN) python -m mission_control.bootstrap.preflight

server: ## Start the scoped Mission Control API (uvicorn; RELOAD=1 for autoreload)
	$(RUN) uvicorn mission_control.bootstrap.api:create_app $(UVICORN_FLAGS)

worker: ## Start the installation-bound Temporal worker
	$(RUN) python -m mission_control.bootstrap.worker

agent-server: ## Start the bounded Agent Server (langgraph dev)
	$(RUN) langgraph dev --config agent_server/langgraph.json --host $(HOST) --port $(AGENT_PORT) --no-browser

mcp-server: ## Start the Coordinator FastMCP server (Streamable HTTP) for Cursor/dashboard testing
	$(RUN) python -m mission_control.interfaces.mcp --host $(HOST) --port $(MCP_PORT) --path $(MCP_PATH) $(ARGS)

health: ## Probe the API liveness and readiness endpoints
	curl -fsS http://$(HOST):$(PORT)/health/live && echo
	curl -fsS http://$(HOST):$(PORT)/health/ready && echo

openapi: ## Download the OpenAPI document to .scratch/openapi.json
	@mkdir -p .scratch
	curl -fsS http://$(HOST):$(PORT)/openapi.json -o .scratch/openapi.json
	@echo "wrote .scratch/openapi.json"

socketio-smoke: ## Run the Socket.IO smoke client against the API
	$(RUN) python scripts/socketio_smoke.py

missionctl: ## Run the authenticated CLI, e.g. make missionctl ARGS="run inspect RUN_ID"
	$(RUN) missionctl $(ARGS)

# ========================================================================================
##@ Database component (mission-db; APP=biotech|ai-engineer, read-only unless stated)

mission-db: ## Run any mission-db subcommand, e.g. make mission-db ARGS="--help"
	$(RUN) mission-db $(ARGS)

db-inspect: ## Inspect the target manifest and lock for APP
	$(RUN) mission-db inspect --deployment-dir $(DEPLOYMENT_DIR)

db-plan: ## Produce a read-only install plan for APP -> .scratch/mission-db/plan-<APP>.json
	@mkdir -p $(OUT_DIR)
	$(RUN) mission-db plan --deployment-dir $(DEPLOYMENT_DIR) --out $(OUT_DIR)/plan-$(APP).json

db-verify: ## Verify the installed common component for APP
	$(RUN) mission-db verify --deployment-dir $(DEPLOYMENT_DIR)

db-runtime-plan: ## Plan the LangGraph saver/store runtime phase for APP
	$(RUN) mission-db runtime-plan --deployment-dir $(DEPLOYMENT_DIR) --descriptor $(PKG)/runtime/descriptor.json

db-seed-plan: ## Plan seed bundles for APP (common + app bundle)
	$(RUN) mission-db seed-plan --deployment-dir $(DEPLOYMENT_DIR) $(SEED_BUNDLES)

db-snapshot: ## Hash-only snapshot of the installed component -> .scratch/mission-db/snapshot-<APP>.json
	@mkdir -p $(OUT_DIR)
	$(RUN) mission-db snapshot --deployment-dir $(DEPLOYMENT_DIR) --out $(OUT_DIR)/snapshot-$(APP).json

# Writes to a live target. Requires owner authorization plus the plan digest and the
# project_ref:installation_id confirmation; without them this target refuses to run.
db-apply: ## APPLY the plan (needs PLAN_DIGEST=... CONFIRM_TARGET=project_ref:installation_id)
ifndef PLAN_DIGEST
	$(error db-apply needs PLAN_DIGEST (plan_digest from `make db-plan`))
endif
ifndef CONFIRM_TARGET
	$(error db-apply needs CONFIRM_TARGET=<project_ref>:<installation_id>)
endif
	$(RUN) mission-db apply --deployment-dir $(DEPLOYMENT_DIR) --expected-plan-digest $(PLAN_DIGEST) --confirm-target $(CONFIRM_TARGET)

# ========================================================================================
##@ Quality

fmt: ## Format code and apply safe lint fixes (ruff)
	$(RUN) ruff format .
	$(RUN) ruff check . --fix

fmt-check: ## Fail if formatting would change anything
	$(RUN) ruff format --check .

lint: ## Lint without modifying files
	$(RUN) ruff check .

lint-fix: ## Lint and apply safe fixes only
	$(RUN) ruff check . --fix

typecheck: ## Authoritative type gate (mypy, ~1-2 min cold, incremental afterwards)
	$(RUN) mypy

typecheck-fast: ## Fast type feedback (ty, seconds)
	$(RUN) ty check

typecheck-watch: ## Re-run ty on every file change
	$(RUN) ty check --watch

typecheck-daemon: ## Incremental mypy via the dmypy daemon (fast re-runs)
	$(RUN) dmypy run -- src/mission_control

deps-check: ## Detect undeclared, unused or transitive-only imports (deptry)
	$(RUN) deptry src

audit: ## Scan uv.lock for known vulnerabilities (uv-secure via uvx)
	$(UV)x uv-secure uv.lock

links: ## Validate relative Markdown links in docs
	$(RUN) python docs/tools/check_links.py

precommit: ## Run every pre-commit hook against all files
	$(RUN) prek run --all-files

arch: test-arch ## Alias: architecture boundary tests

skills-manifest: ## Rewrite skills/*/manifest.json digests from the files on disk
	$(RUN) python scripts/skills_manifest.py --write

skills-check: ## Fail if any skills/*/manifest.json digest drifted from disk
	$(RUN) python scripts/skills_manifest.py --check

seeds-validate: ## Fail if a seed Capability Pin does not parse or a tools/list digest drifted
	$(RUN) python scripts/seeds_validate.py

check: lint fmt-check typecheck-fast deps-check test-arch test-unit skills-check ## Fast local gate (seconds to a minute)

ci: lock-check lint fmt-check typecheck deps-check test-arch test-unit links ## Full gate: everything that needs no live infrastructure

# ========================================================================================
##@ Tests (PYTEST_ARGS="..." passes through)

test: ## Full suite with the biotech group (needs PostgreSQL/Temporal for live selections)
	$(UV) run --group biotech pytest $(PYTEST_ARGS)

test-unit: ## Unit tests
	$(PYTEST) tests/unit

test-unit-fast: ## Unit tests in parallel (pytest-xdist)
	$(PYTEST) tests/unit -n auto

test-arch: ## Package-boundary and ownership rules
	$(PYTEST) tests/architecture

test-integration: ## Integration tests (require compose services)
	$(PYTEST) tests/integration

test-acceptance: ## Acceptance drills (require compose services and configured credentials)
	$(PYTEST) tests/acceptance

test-qualification: ## Two-project qualification (needs MISSION_CONTROL_TEST_ADMIN_DSN)
	$(PYTEST) tests/qualification/two_project

test-db-contract: ## mission-control-db-contract package tests
	$(RUN) pytest $(PKG)/tests $(PYTEST_ARGS)

test-failed: ## Re-run only the tests that failed last time
	$(PYTEST) --lf

coverage: ## Unit-test coverage report (terminal + htmlcov/)
	$(PYTEST) tests/unit --cov --cov-report=term-missing --cov-report=html

# ========================================================================================
##@ Housekeeping and composition

clean: ## Remove tool caches, coverage output and __pycache__ (never .venv or volumes)
	$(RUN) python scripts/dev/clean_caches.py

dev: infra-up ## Infra up + wait, then print the next commands
	@echo ""
	@echo "Infrastructure is ready. In separate terminals:"
	@echo "  make server        # API on http://$(HOST):$(PORT)  (docs at /docs)"
	@echo "  make worker        # Temporal worker"
	@echo "  make agent-server  # Agent Server on http://$(HOST):$(AGENT_PORT)"
	@echo "  make mcp-server    # Coordinator MCP on http://$(HOST):$(MCP_PORT)$(MCP_PATH)"
	@echo "  Temporal UI: http://127.0.0.1:$(TEMPORAL_UI_PORT)"

up: infra-up preflight server ## infra-up -> preflight -> server

down: infra-down ## Alias for infra-down

stop: infra-down ## Alias for infra-down

status: infra-ps ## Compose status plus API health (if the server is running)
	-@$(MAKE) --no-print-directory health
