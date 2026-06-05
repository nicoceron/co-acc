ifneq (,$(wildcard .env))
include .env
export $(shell sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' .env)
endif

LAKE_ROOT ?= $(abspath $(CURDIR))/lake
LAKE_REALITY_ARGS = $(if $(DATASET),--dataset $(DATASET),) \
	$(if $(DATASETS),--datasets "$(DATASETS)",) \
	$(if $(WITH_LIVE),--with-live,) \
	$(if $(BASELINE),--baseline $(BASELINE),) \
	$(if $(REALITY_DATE),--date $(REALITY_DATE),) \
	$(if $(REALITY_OUTPUT_DIR),--output-dir "$(REALITY_OUTPUT_DIR)",) \
	$(if $(REALITY_THRESHOLDS),--thresholds "$(REALITY_THRESHOLDS)",) \
	$(if $(CHANGED_YAMLS_ONLY),--changed-yamls-only,) \
	$(if $(CURATED_TABLE),--curated-table "$(CURATED_TABLE)",) \
	$(if $(CURATED_TABLES),--curated-tables "$(CURATED_TABLES)",) \
	$(if $(SKIP_CURATED),--skip-curated,) \
	$(if $(CURATED_ONLY),--curated-only,)

.PHONY: setup-env dev stop api etl frontend \
	clean-data \
	lint type-check format \
	test test-api test-etl test-frontend check \
	lake-init lake-reality lake-compact curated-contracts api-smoke backend-ready \
	lake-backup lake-backup-verify lake-backup-restore \
	prod-smoke \
	qualify ingest ingest-all ingest-phase7-smoke ingest-phase7-full curate \
	materialize-deps materialize-all

# ---------------------------------------------------------------------------
# Setup / dev
# ---------------------------------------------------------------------------

setup-env:
	bash scripts/init_env.sh

dev:
	docker compose up -d

stop:
	docker compose down

api:
	cd api && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run python -m uvicorn coacc.main:app --reload --host 0.0.0.0 --port 8000

etl:
	cd etl && uv run coacc-etl --help

frontend:
	cd frontend && npm run dev

clean-data:
	find data -mindepth 1 -maxdepth 1 -not -name ".gitkeep" -exec rm -rf {} +
	@echo "Data directory cleaned."

# ---------------------------------------------------------------------------
# Lint / type / test
# ---------------------------------------------------------------------------

lint:
	cd api && uv run ruff check src/ tests/
	cd etl && uv run ruff check src/ tests/
	cd frontend && npm run lint

format:
	cd etl && uv run ruff format src/ tests/
	cd api && uv run ruff format src/ tests/

type-check:
	cd api && uv run mypy src/
	cd etl && uv run mypy src/
	cd frontend && npm run type-check

test-api:
	cd api && uv run pytest

test-etl:
	cd etl && uv run pytest

test-frontend:
	cd frontend && npm test

test: test-api test-etl test-frontend

check: lint type-check test

# ---------------------------------------------------------------------------
# Lake / ingest (post-Wave-4 architecture)
# ---------------------------------------------------------------------------

lake-init:
	mkdir -p $(LAKE_ROOT)/raw $(LAKE_ROOT)/curated $(LAKE_ROOT)/meta

lake-reality:
	cd etl && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run python ../scripts/lake_reality.py $(LAKE_REALITY_ARGS)

lake-compact:
	cd etl && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run python -m coacc_etl.lakehouse.compactor --older-than=30d

curate: lake-init
	cd etl && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run coacc-etl curate $(if $(TABLE),--table $(TABLE),)

curated-contracts:
	cd etl && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run python ../scripts/check_curated_contracts.py $(if $(TABLE),--table $(TABLE),)

api-smoke:
	python3 scripts/local_backend_smoke.py --lake-root "$(LAKE_ROOT)"

prod-smoke:
	python3 scripts/production_smoke.py $(if $(BASE_URL),--base-url "$(BASE_URL)",)

backend-ready: lake-init
	docker compose config >/dev/null
	docker compose --profile etl --profile ops config >/dev/null
	$(MAKE) curated-contracts
	$(MAKE) lake-reality
	$(MAKE) api-smoke

lake-backup:
	COACC_HOST_LAKE_ROOT="$(LAKE_ROOT)" infra/scripts/backup-lake.sh

lake-backup-verify:
	@test -n "$(ARCHIVE)" || (echo "ARCHIVE=/path/to/coacc_lake_*.tar.gz is required"; exit 1)
	infra/scripts/verify-lake-backup.sh "$(ARCHIVE)"

lake-backup-restore:
	@test -n "$(ARCHIVE)" || (echo "ARCHIVE=/path/to/coacc_lake_*.tar.gz is required"; exit 1)
	@test -n "$(RESTORE_ROOT)" || (echo "RESTORE_ROOT=/path/to/restore is required"; exit 1)
	infra/scripts/restore-lake-backup.sh "$(ARCHIVE)" "$(RESTORE_ROOT)"

# Qualify (re-)builds the signed catalog. Pass extra flags via QUALIFY_ARGS.
qualify:
	cd etl && uv run coacc-etl qualify $(QUALIFY_ARGS)

# Ingest a single dataset by Socrata 4x4 id, e.g.
#   make ingest DATASET=8qxx-ubmq
# Optional: FULL_REFRESH=1 to bypass the lake watermark.
ingest:
	@test -n "$(DATASET)" || (echo "DATASET=<id> is required"; exit 1)
	cd etl && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run coacc-etl ingest $(DATASET) $(if $(FULL_REFRESH),--full-refresh,)

# Ingest every tier=core dataset that is YAML-ready, in dep-safe order.
ingest-all:
	cd etl && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run coacc-etl ingest-all $(if $(FULL_REFRESH),--full-refresh,) $(if $(CONTINUE_ON_ERROR),--continue-on-error,)

# Phase 7 operator wrappers. Pass extra flags via PHASE7_ARGS, e.g.
#   make ingest-phase7-smoke PHASE7_ARGS="--dataset wi7w-2nvm --continue-on-error"
#   make ingest-phase7-full PHASE7_ARGS="--min-free-gb 80"
ingest-phase7-smoke: lake-init
	cd etl && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run coacc-etl ingest-phase7 --mode smoke --continue-on-error $(PHASE7_ARGS)

ingest-phase7-full: lake-init
	cd etl && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run coacc-etl ingest-phase7 --mode full $(PHASE7_ARGS)

# ---------------------------------------------------------------------------
# Downstream signal materialization from curated parquet
# ---------------------------------------------------------------------------

materialize-deps: materialize-all

materialize-all: lake-init
	cd etl && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run coacc-etl signals materialize --all $(if $(RUN_ID),--run-id "$(RUN_ID)",) $(if $(ALLOW_EMPTY),--allow-empty,)
