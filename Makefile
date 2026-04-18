ifneq (,$(wildcard .env))
include .env
export $(shell sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' .env)
endif

.PHONY: dev stop api etl frontend demo demo-national demo-bogota demo-synthetic build-watchlist-snapshots materialize-results scan-real-pattern-coverage clean-data seed lint type-check test test-api test-etl test-frontend check lake-init lake-pipeline lake-compact materialize-deps materialize-all \
	validate-known-cases \
	sync-colombia-registry generate-pipeline-status generate-source-summary \
	download-secop-integrado download-secop-sanciones download-secop-proveedores \
	download-secop-procesos download-secop-ofertas download-secop-contratos \
	download-secop-compromisos download-secop-solicitudes-cdp download-secop-facturas \
	download-secop-ubicaciones-ejecucion download-sigep-servidores \
	download-sigep-cargos-sensibles download-ley2013-activos download-ley2013-conflictos \
	download-secop-ejecucion download-secop-adiciones download-secop-modificaciones download-secop-suspensiones \
	download-secop-plan-pagos download-secop-bpin download-secop-rubros download-secop-interadmin \
	download-secop-ubicaciones-adicionales download-connected-secop-i-history \
	download-connected-secop-scope \
	download-connected-company-registry \
	probe-colombia-candidates \
	download-reps-salud download-men-matricula download-sgr-proyectos download-sgr-gastos \
	download-cuentas-claras-2019 download-paco-sanciones download-pte-compromisos-sector \
	download-siri-antecedentes download-responsabilidad-fiscal download-hallazgos-fiscales \
	download-pte-contratos-grandes download-mapa-proyectos download-rues-camaras \
	download-supersoc-1000 \
	download-igac-transacciones \
	etl-secop-integrado etl-secop-sanciones etl-secop-proveedores etl-secop-procesos \
	etl-secop-ofertas etl-secop-contratos etl-secop-compromisos etl-secop-solicitudes-cdp \
	etl-secop-facturas etl-secop-ubicaciones-ejecucion etl-sigep-servidores etl-sigep-cargos-sensibles \
	etl-ley2013-activos etl-ley2013-conflictos etl-secop-ejecucion etl-secop-adiciones \
	etl-secop-modificaciones etl-secop-suspensiones etl-secop-plan-pagos etl-secop-bpin etl-secop-rubros etl-secop-interadmin etl-secop-ubicaciones-adicionales etl-secop-i-historico etl-secop-i-origen-recursos etl-reps-salud etl-men-matricula etl-sgr-proyectos \
	etl-sgr-gastos etl-cuentas-claras-2019 etl-paco-sanciones etl-pte-compromisos-sector \
	etl-siri-antecedentes etl-responsabilidad-fiscal etl-hallazgos-fiscales \
	etl-pte-contratos-grandes etl-mapa-proyectos etl-rues-camaras \
	etl-registraduria-checks etl-supersoc-1000 \
	etl-igac-transacciones etl-company-registry-c82u etl-all

NATIONAL_PROCUREMENT_SOURCES := secop_integrado,secop_sanctions,secop_suppliers,secop_ii_processes,secop_offers,secop_ii_contracts,secop_invoices,secop_payment_plans,secop_contract_execution,secop_contract_additions,secop_contract_suspensions,secop_interadmin_agreements,sigep_public_servants,sigep_sensitive_positions,asset_disclosures,conflict_disclosures,health_providers,cuentas_claras_income_2019,paco_sanctions,siri_antecedents,fiscal_responsibility,fiscal_findings,sgr_projects,sgr_expense_execution
LAKE_ROOT ?= $(abspath $(CURDIR))/lake

setup-env:
	bash scripts/init_env.sh

# Demo Bogotá: Targeted Real Data from Bogotá D.C.
# This ensures overlaps between Politicians and Contracts by geography.
demo-bogota:
	docker compose down -v --remove-orphans
	docker compose up -d --build
	@echo "Waiting for Neo4j..."
	@until docker exec coacc-neo4j cypher-shell -u neo4j -p "$${NEO4J_PASSWORD:-}" "RETURN 1" >/dev/null 2>&1; do sleep 2; done
	$(MAKE) clean-data
	@echo "Downloading targeted Bogotá data..."
	# SIGEP: Public servants in Bogota entities
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id 2jzx-383z --output ../data/sigep_public_servants/sigep_public_servants.csv --where "nombreentidad like '%BOGOT%' or idmunicipioentidad = '11001'" --mode paged-json
	# SIGEP: Sensitive positions in Bogota entities
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id 5u9e-g5w9 --output ../data/sigep_sensitive_positions/sigep_sensitive_positions.csv --where "nombre_institucion like '%BOGOT%' or municipio_institucion like '%Bogot%' or dpto_institucion like '%BOGOT%'" --mode paged-json
	# SECOP II: Contract-centric Bogotá slice plus related offers, suppliers, invoices, commitments, execution, additions, and modifications
	cd etl && uv run python scripts/download_bogota_secop_scope.py --data-dir ../data
	# Ley 2013: Conflicts and Assets for Bogota entities (Crucial for person connections)
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id gbry-rnq4 --output ../data/conflict_disclosures/conflict_disclosures.csv --where "nombre_entidad like '%BOGOT%'" --mode paged-json
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id 8tz7-h3eu --output ../data/asset_disclosures/asset_disclosures.csv --where "nombre_entidad like '%BOGOT%'" --mode paged-json
	# REPS: Health operators in Bogota
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id c36g-9fc2 --output ../data/health_providers/health_providers.csv --where "municipio_prestador = '11001' or municipiosede = '11001'" --mode paged-json
	# Campaign finance: Bogota territorial elections
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id jgra-rz2t --output ../data/cuentas_claras_income_2019/cuentas_claras_income_2019.csv --where "dep_nombre = 'BOGOTA D.C.' or mun_nombre = 'BOGOTA D.C.'" --mode paged-json
	# PACO: Sanctions (not filterable by city easily, download all)
	cd etl && uv run python scripts/download_paco_dataset.py --output-dir ../data/paco_sanctions
	@echo "Ingesting data..."
	cd etl && uv run coacc-etl run --source sigep_public_servants --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source sigep_sensitive_positions --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source secop_ii_processes --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source secop_ii_contracts --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source secop_offers --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source secop_suppliers --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source secop_invoices --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source secop_payment_plans --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source secop_contract_execution --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source secop_contract_additions --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source secop_budget_items --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source secop_process_bpin --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source secop_contract_suspensions --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source secop_interadmin_agreements --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source conflict_disclosures --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source asset_disclosures --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source health_providers --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source cuentas_claras_income_2019 --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	cd etl && uv run coacc-etl run --source paco_sanctions --neo4j-password "$${NEO4J_PASSWORD:-}" --data-dir ../data
	$(MAKE) build-watchlist-snapshots
	@echo "Demo Bogotá ready. Visit http://localhost:3000"

# Demo: National procurement-first real-data graph.
# RUN THIS: make demo
demo: demo-national

demo-national:
	$(MAKE) clean-data
	bash scripts/bootstrap_all_public.sh --yes-reset --noninteractive --sources $(NATIONAL_PROCUREMENT_SOURCES)
	$(MAKE) build-watchlist-snapshots
	@echo "National procurement demo ready. Visit http://localhost:3000"

# Synthetic regression/demo seed kept as an explicit fallback target.
demo-synthetic:
	docker compose down -v --remove-orphans
	docker compose up -d --build
	@echo "Waiting for Neo4j..."
	@until docker exec coacc-neo4j cypher-shell -u neo4j -p "$${NEO4J_PASSWORD:-}" "RETURN 1" >/dev/null 2>&1; do sleep 2; done
	bash infra/scripts/seed-dev.sh
	@echo "Demo environment ready. Visit http://localhost:3000"

clean-data:
	find data -mindepth 1 -maxdepth 1 -not -name ".gitkeep" -exec rm -rf {} +
	@echo "Data directory cleaned."

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

sync-colombia-registry:
	python3 scripts/sync_colombia_portal_registry.py

generate-pipeline-status:
	python3 scripts/generate_pipeline_status.py --registry-path docs/source_registry_co_v1.csv --output docs/pipeline_status.md --api-base http://localhost:8000

generate-source-summary:
	python3 scripts/generate_data_sources_summary.py --registry-path docs/source_registry_co_v1.csv --docs-path docs/data-sources.md --api-base http://localhost:8000

build-watchlist-snapshots:
	cd api && uv run python scripts/build_watchlist_snapshots.py

materialize-results: build-watchlist-snapshots
	python3 scripts/materialize_real_results.py --api-base http://localhost:8000 --output frontend/public/data/materialized-results.json --mirror audit-results/materialized-results/latest/materialized-results.json

scan-real-pattern-coverage:
	python3 scripts/scan_real_pattern_coverage.py --api-base http://localhost:8000 --output docs/real_pattern_coverage_2026-03-21.md

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

download-secop-suspensiones:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id u99c-7mfm --output ../data/secop_contract_suspensions/secop_contract_suspensions.csv

download-secop-plan-pagos:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id uymx-8p3j --output ../data/secop_payment_plans/secop_payment_plans.csv

download-secop-bpin:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id d9na-abhe --output ../data/secop_process_bpin/secop_process_bpin.csv

download-secop-rubros:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id cwhv-7fnp --output ../data/secop_budget_items/secop_budget_items.csv

download-secop-interadmin:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id s484-c9k3 --output ../data/secop_interadmin_agreements/secop_interadmin_agreements.csv
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id ityv-bxct --output ../data/secop_interadmin_agreements_historical/secop_interadmin_agreements_historical.csv

download-connected-secop-scope:
	cd etl && uv run python scripts/download_connected_secop_scope.py --data-dir ../data

download-connected-secop-i-history:
	cd etl && uv run python scripts/download_connected_secop_i_history.py --data-dir ../data

download-connected-company-registry:
	cd etl && uv run python scripts/download_connected_company_registry.py --data-dir ../data

download-secop-ubicaciones-adicionales:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id wwhe-4sq8 --output ../data/secop_additional_locations/secop_additional_locations.csv

download-secop-archivos-descarga:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id dmgg-8hin --output ../data/secop_document_archives/secop_document_archives.csv

probe-colombia-candidates:
	cd etl && uv run python ../scripts/probe_colombia_candidate_datasets.py

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

download-siri-antecedentes:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id iaeu-rcn6 --output ../data/siri_antecedents/siri_antecedents.csv

download-responsabilidad-fiscal:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id jr8e-e8tu --output ../data/fiscal_responsibility/fiscal_responsibility.csv

download-hallazgos-fiscales:
	cd etl && uv run python scripts/download_socrata_dataset.py --dataset-id 8qxx-ubmq --output ../data/fiscal_findings/fiscal_findings.csv

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

etl-secop-ubicaciones-adicionales:
	cd etl && uv run coacc-etl run --source secop_additional_locations --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-archivos-descarga:
	cd etl && uv run coacc-etl run --source secop_document_archives --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-i-historico:
	cd etl && uv run coacc-etl run --source secop_i_historical_processes --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-i-origen-recursos:
	cd etl && uv run coacc-etl run --source secop_i_resource_origins --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

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

etl-secop-suspensiones:
	cd etl && uv run coacc-etl run --source secop_contract_suspensions --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-plan-pagos:
	cd etl && uv run coacc-etl run --source secop_payment_plans --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-bpin:
	cd etl && uv run coacc-etl run --source secop_process_bpin --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-rubros:
	cd etl && uv run coacc-etl run --source secop_budget_items --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

etl-secop-interadmin:
	cd etl && uv run coacc-etl run --source secop_interadmin_agreements --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 5000

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

etl-siri-antecedentes:
	cd etl && uv run coacc-etl run --source siri_antecedents --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-responsabilidad-fiscal:
	cd etl && uv run coacc-etl run --source fiscal_responsibility --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-hallazgos-fiscales:
	cd etl && uv run coacc-etl run --source fiscal_findings --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

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

etl-company-registry-c82u:
	cd etl && uv run coacc-etl run --source company_registry_c82u --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 10000

download-company-branches-nb3d:
	cd etl && uv run python scripts/download_connected_company_branches.py --data-dir ../data

etl-company-branches-nb3d:
	cd etl && uv run coacc-etl run --source company_branches_nb3d --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data --streaming --chunk-size 10000

download-dnp-bpin-wave:
	cd etl && uv run python scripts/download_connected_dnp_bpin_wave.py --data-dir ../data

etl-dnp-project-executors:
	cd etl && uv run coacc-etl run --source dnp_project_executors --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-dnp-project-beneficiary-locations:
	cd etl && uv run coacc-etl run --source dnp_project_beneficiary_locations --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-dnp-project-beneficiary-characterization:
	cd etl && uv run coacc-etl run --source dnp_project_beneficiary_characterization --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-dnp-project-locations:
	cd etl && uv run coacc-etl run --source dnp_project_locations --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-dnp-project-contract-links:
	cd etl && uv run coacc-etl run --source dnp_project_contract_links --neo4j-password "$${NEO4J_PASSWORD}" --data-dir ../data

etl-all: etl-secop-integrado etl-secop-sanciones etl-secop-proveedores etl-secop-procesos etl-secop-ofertas etl-secop-contratos etl-secop-compromisos etl-secop-solicitudes-cdp etl-secop-facturas etl-secop-plan-pagos etl-secop-bpin etl-secop-rubros etl-secop-ubicaciones-ejecucion etl-secop-ubicaciones-adicionales etl-secop-i-historico etl-secop-i-origen-recursos etl-sigep-servidores etl-sigep-cargos-sensibles etl-ley2013-activos etl-ley2013-conflictos etl-secop-ejecucion etl-secop-adiciones etl-secop-modificaciones etl-secop-suspensiones etl-secop-interadmin etl-reps-salud etl-men-matricula etl-sgr-proyectos etl-sgr-gastos etl-cuentas-claras-2019 etl-paco-sanciones etl-siri-antecedentes etl-responsabilidad-fiscal etl-hallazgos-fiscales etl-pte-compromisos-sector etl-pte-contratos-grandes etl-mapa-proyectos etl-rues-camaras etl-supersoc-1000 etl-igac-transacciones etl-company-branches-nb3d etl-dnp-project-executors etl-dnp-project-beneficiary-locations etl-dnp-project-beneficiary-characterization etl-dnp-project-locations etl-dnp-project-contract-links

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

validate-known-cases:
	curl -s 'http://localhost:8000/api/v1/meta/validation/known-cases' | jq '.'

lake-init:
	mkdir -p $(LAKE_ROOT)/raw $(LAKE_ROOT)/curated $(LAKE_ROOT)/meta

lake-pipeline:
	cd etl && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run python -m coacc_etl.runner run --pipeline="$(PIPELINE)" --to-lake --data-dir ../data

lake-compact:
	cd etl && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run python -m coacc_etl.lakehouse.compactor --older-than=30d

materialize-deps:
	cd api && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run python -m coacc.services.signal_materializer --advanced-sources="$(SOURCES)"

materialize-all:
	cd api && COACC_LAKE_ROOT="$(LAKE_ROOT)" uv run python -m coacc.services.signal_materializer --all
