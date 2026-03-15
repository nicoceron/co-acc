ifneq (,$(wildcard .env))
include .env
export $(shell sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' .env)
endif

.PHONY: dev stop api etl frontend seed lint type-check test test-api test-etl test-frontend check \
	sync-colombia-registry generate-pipeline-status generate-source-summary \
	download-secop-integrado download-secop-sanciones download-secop-proveedores \
	download-secop-procesos download-secop-ofertas download-secop-contratos \
	download-secop-compromisos download-secop-solicitudes-cdp download-secop-facturas \
	download-secop-ubicaciones-ejecucion download-sigep-servidores \
	download-sigep-cargos-sensibles download-ley2013-activos download-ley2013-conflictos \
	download-secop-ejecucion download-secop-adiciones download-secop-modificaciones \
	download-reps-salud download-men-matricula download-sgr-proyectos download-sgr-gastos \
	download-cuentas-claras-2019 download-paco-sanciones download-pte-compromisos-sector \
	download-pte-contratos-grandes download-mapa-proyectos download-rues-camaras \
	download-supersoc-1000 \
	download-igac-transacciones \
	etl-secop-integrado etl-secop-sanciones etl-secop-proveedores etl-secop-procesos \
	etl-secop-ofertas etl-secop-contratos etl-secop-compromisos etl-secop-solicitudes-cdp \
	etl-secop-facturas etl-secop-ubicaciones-ejecucion etl-sigep-servidores etl-sigep-cargos-sensibles \
	etl-ley2013-activos etl-ley2013-conflictos etl-secop-ejecucion etl-secop-adiciones \
	etl-secop-modificaciones etl-reps-salud etl-men-matricula etl-sgr-proyectos \
	etl-sgr-gastos etl-cuentas-claras-2019 etl-paco-sanciones etl-pte-compromisos-sector \
	etl-pte-contratos-grandes etl-mapa-proyectos etl-rues-camaras \
	etl-registraduria-checks etl-supersoc-1000 \
	etl-igac-transacciones etl-all etl-hot-data etl-full-data

setup-env:
	bash scripts/init_env.sh

# Hot Data: High-signal, recent (2024-2026) data for quick analysis on limited hardware.
etl-hot-data: etl-paco-sanciones etl-sigep-cargos-sensibles etl-secop-sanciones etl-ley2013-activos etl-ley2013-conflictos
	@echo "Hot data load complete. Focus: Sanctions, Sensitive Positions, and Disclosures."

# Full Data: Complete historical sync (requires 200GB+ disk and 32GB+ RAM).
etl-full-data: etl-all
	@echo "Full historical data load complete."

dev:
	docker compose up -d

stop:
	docker compose down

api:
	cd api && uv run uvicorn coacc.main:app --reload --host 0.0.0.0 --port 8000

etl:
	cd etl && uv run coacc-etl --help

frontend:
	cd frontend && npm run dev

seed:
	bash infra/scripts/seed-dev.sh

sync-colombia-registry:
	python3 scripts/sync_colombia_portal_registry.py

generate-pipeline-status:
	python3 scripts/generate_pipeline_status.py --registry-path docs/source_registry_co_v1.csv --output docs/pipeline_status.md

generate-source-summary:
	python3 scripts/generate_data_sources_summary.py --registry-path docs/source_registry_co_v1.csv --docs-path docs/data-sources.md

download-secop-integrado:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id rpmr-utcd --output ../data/secop_integrado/secop_integrado.csv

download-secop-sanciones:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id 4n4q-k399 --output ../data/secop_sanctions/secop_i_sanctions.csv
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id it5q-hg94 --output ../data/secop_sanctions/secop_ii_sanctions.csv

download-secop-proveedores:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id qmzu-gj57 --output ../data/secop_suppliers/secop_suppliers.csv

download-secop-procesos:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id p6dx-8zbt --output ../data/secop_ii_processes/secop_ii_processes.csv

download-secop-ofertas:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id wi7w-2nvm --output ../data/secop_offers/secop_offers.csv

download-secop-contratos:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id jbjy-vk9h --output ../data/secop_ii_contracts/secop_ii_contracts.csv

download-secop-compromisos:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id skc9-met7 --output ../data/secop_budget_commitments/secop_budget_commitments.csv

download-secop-solicitudes-cdp:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id a86w-fh92 --output ../data/secop_cdp_requests/secop_cdp_requests.csv

download-secop-facturas:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id ibyt-yi2f --output ../data/secop_invoices/secop_invoices.csv

download-secop-ubicaciones-ejecucion:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id gra4-pcp2 --output ../data/secop_execution_locations/secop_execution_locations.csv

download-sigep-servidores:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id 2jzx-383z --output ../data/sigep_public_servants/sigep_public_servants.csv

download-sigep-cargos-sensibles:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id 5u9e-g5w9 --output ../data/sigep_sensitive_positions/sigep_sensitive_positions.csv

download-ley2013-activos:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id 8tz7-h3eu --output ../data/asset_disclosures/asset_disclosures.csv

download-ley2013-conflictos:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id gbry-rnq4 --output ../data/conflict_disclosures/conflict_disclosures.csv

download-secop-ejecucion:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id mfmm-jqmq --output ../data/secop_contract_execution/secop_contract_execution.csv

download-secop-adiciones:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id cb9c-h8sn --output ../data/secop_contract_additions/secop_contract_additions.csv

download-secop-modificaciones:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id u8cx-r425 --output ../data/secop_contract_modifications/secop_contract_modifications.csv

download-reps-salud:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id c36g-9fc2 --output ../data/health_providers/health_providers.csv

download-men-matricula:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id 5wck-szir --output ../data/higher_ed_enrollment/higher_ed_enrollment.csv

download-sgr-proyectos:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id mzgh-shtp --output ../data/sgr_projects/sgr_projects.csv

download-sgr-gastos:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id qkv4-ek54 --output ../data/sgr_expense_execution/sgr_expense_execution.csv

download-cuentas-claras-2019:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id jgra-rz2t --output ../data/cuentas_claras_income_2019/cuentas_claras_income_2019.csv

download-paco-sanciones:
	cd etl && uv run python scripts/download_paco_dataset.py --output-dir ../data/paco_sanctions

download-pte-compromisos-sector:
	cd etl && uv run python scripts/download_pte_export.py --page sector-commitments --output ../data/pte_sector_commitments/pte_sector_commitments.csv

download-pte-contratos-grandes:
	cd etl && uv run python scripts/download_pte_export.py --page top-contracts --output ../data/pte_top_contracts/pte_top_contracts.csv

download-mapa-proyectos:
	cd etl && uv run python scripts/download_mapa_inversiones_report.py --report project-basics --output ../data/mapa_inversiones_projects/mapa_inversiones_projects.csv

download-rues-camaras:
	cd etl && uv run python scripts/download_rues_chambers.py --output ../data/rues_chambers/rues_chambers.csv

download-supersoc-1000:
	cd etl && uv run python scripts/download_supersoc_top_companies.py --output ../data/supersoc_top_companies/supersoc_top_companies.csv

download-igac-transacciones:
	cd etl && uv run python scripts/download_igac_property_transactions.py --year 2023 --output ../data/igac_property_transactions/igac_property_transactions.csv

etl-secop-integrado:
	cd etl && uv run coacc-etl run --source secop_integrado --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-sanciones:
	cd etl && uv run coacc-etl run --source secop_sanctions --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-secop-proveedores:
	cd etl && uv run coacc-etl run --source secop_suppliers --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-procesos:
	cd etl && uv run coacc-etl run --source secop_ii_processes --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-ofertas:
	cd etl && uv run coacc-etl run --source secop_offers --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-contratos:
	cd etl && uv run coacc-etl run --source secop_ii_contracts --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-compromisos:
	cd etl && uv run coacc-etl run --source secop_budget_commitments --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-solicitudes-cdp:
	cd etl && uv run coacc-etl run --source secop_cdp_requests --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-facturas:
	cd etl && uv run coacc-etl run --source secop_invoices --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-ubicaciones-ejecucion:
	cd etl && uv run coacc-etl run --source secop_execution_locations --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-sigep-servidores:
	cd etl && uv run coacc-etl run --source sigep_public_servants --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-sigep-cargos-sensibles:
	cd etl && uv run coacc-etl run --source sigep_sensitive_positions --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-ley2013-activos:
	cd etl && uv run coacc-etl run --source asset_disclosures --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-ley2013-conflictos:
	cd etl && uv run coacc-etl run --source conflict_disclosures --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-secop-ejecucion:
	cd etl && uv run coacc-etl run --source secop_contract_execution --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-adiciones:
	cd etl && uv run coacc-etl run --source secop_contract_additions --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-modificaciones:
	cd etl && uv run coacc-etl run --source secop_contract_modifications --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-reps-salud:
	cd etl && uv run coacc-etl run --source health_providers --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-men-matricula:
	cd etl && uv run coacc-etl run --source higher_ed_enrollment --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-sgr-proyectos:
	cd etl && uv run coacc-etl run --source sgr_projects --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-sgr-gastos:
	cd etl && uv run coacc-etl run --source sgr_expense_execution --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-cuentas-claras-2019:
	cd etl && uv run coacc-etl run --source cuentas_claras_income_2019 --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-paco-sanciones:
	cd etl && uv run coacc-etl run --source paco_sanctions --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-pte-compromisos-sector:
	cd etl && uv run coacc-etl run --source pte_sector_commitments --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-pte-contratos-grandes:
	cd etl && uv run coacc-etl run --source pte_top_contracts --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-mapa-proyectos:
	cd etl && uv run coacc-etl run --source mapa_inversiones_projects --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-rues-camaras:
	cd etl && uv run coacc-etl run --source rues_chambers --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-registraduria-checks:
	cd etl && uv run coacc-etl run --source registraduria_death_status_checks --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-supersoc-1000:
	cd etl && uv run coacc-etl run --source supersoc_top_companies --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-igac-transacciones:
	cd etl && uv run coacc-etl run --source igac_property_transactions --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-all: etl-secop-integrado etl-secop-sanciones etl-secop-proveedores etl-secop-procesos etl-secop-ofertas etl-secop-contratos etl-secop-compromisos etl-secop-solicitudes-cdp etl-secop-facturas etl-secop-ubicaciones-ejecucion etl-sigep-servidores etl-sigep-cargos-sensibles etl-ley2013-activos etl-ley2013-conflictos etl-secop-ejecucion etl-secop-adiciones etl-secop-modificaciones etl-reps-salud etl-men-matricula etl-sgr-proyectos etl-sgr-gastos etl-cuentas-claras-2019 etl-paco-sanciones etl-pte-compromisos-sector etl-pte-contratos-grandes etl-mapa-proyectos etl-rues-camaras etl-supersoc-1000 etl-igac-transacciones

lint:
	cd api && uv run ruff check src/ tests/
	cd etl && uv run ruff check src/ tests/
	cd frontend && npm run lint

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
