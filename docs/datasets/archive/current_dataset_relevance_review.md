# Dataset relevance review

Generated locally on 2026-04-20 from repository artifacts only. No live Socrata metadata fetch was run.

## What is actually present right now

- `data/` contains only `.gitkeep`; there are no raw CSV dataset outputs to inspect in the workspace.
- `lake/raw`, `lake/curated`, and `lake/meta` also contain only `.gitkeep`; there is no local Parquet lake output.
- The only substantial generated output is `frontend/public/data/materialized-results.json`, generated at `2026-03-28T21:50:42Z` plus `1200` case JSON files.
- That generated output reports `52` implemented sources, `51` loaded sources, `51` healthy sources, and `1` blocked external source.
- The operational registry has `52` current sources in `docs/source_registry_co_v1.csv`.
- The ETL runner exposes `106` pipeline keys; `53` are not in the current registry and should be treated as scaffolds or stale code unless signal deps require them.
- `config/bootstrap_all_contract.yml` is not valid JSON/YAML at parse time (`Expecting ',' delimiter: line 70 column 3 (char 1964)`); source roles below were extracted defensively from its arrays.

## Output evidence by source

| source | refs |
| --- | --- |
| secop_integrado | 2840 |
| secop_i_historical_processes | 1098 |
| paco_sanctions | 96 |
| higher_ed_enrollment | 96 |
| official_case_bulletins | 70 |
| secop_sanctions | 69 |
| siri_antecedents | 34 |
| secop_cdp_requests | 27 |
| company_registry_c82u | 24 |
| cuentas_claras_income_2019 | 20 |
| higher_ed_directors | 8 |
| icaft_rut_publico_2026 | 6 |
| secop_ii_processes | 5 |
| secop_execution_locations | 4 |
| fiscal_responsibility | 4 |
| secop_payment_plans | 3 |
| secop_suppliers | 2 |
| san_jose_documentos_oficiales | 2 |
| san_jose_politica_seguridad_2024 | 2 |
| icaft_certificado_oficial_2024 | 2 |

These counts are source references inside the saved public output/case JSON, not raw row counts.

## Current 52-source recommendation counts

- `hold`: 1
- `keep`: 24
- `keep_context`: 11
- `keep_or_fix_deps`: 3
- `quarantine`: 1
- `review_context`: 9
- `review_or_drop`: 3

## Current sources

| source_id | role | reco | signals | output_refs | why |
| --- | --- | --- | --- | --- | --- |
| `registraduria_death_status_checks` | enrichment_only | hold | 0 (0 req) | 0 | External/manual access is blocked; not useful operationally until data can be lawfully and repeatably loaded. |
| `company_registry_c82u` | promoted | keep | 4 (4 req) | 24 | Required by 4 signal(s); removing it breaks materialization. |
| `conflict_disclosures` | promoted | keep | 1 (0 req) | 0 | Promoted and visible in dependencies/output (1 signal refs, 0 evidence refs). |
| `cuentas_claras_income_2019` | promoted | keep | 1 (0 req) | 20 | Promoted and visible in dependencies/output (1 signal refs, 20 evidence refs). |
| `fiscal_findings` | promoted | keep | 1 (0 req) | 0 | Promoted and visible in dependencies/output (1 signal refs, 0 evidence refs). |
| `fiscal_responsibility` | promoted | keep | 1 (0 req) | 4 | Promoted and visible in dependencies/output (1 signal refs, 4 evidence refs). |
| `official_case_bulletins` | promoted | keep | 1 (0 req) | 70 | Promoted and visible in dependencies/output (1 signal refs, 70 evidence refs). |
| `paco_sanctions` | promoted | keep | 1 (0 req) | 96 | Core source in bootstrap; it anchors contracts, sanctions, or public-official identity for many signals. |
| `secop_contract_additions` | promoted | keep | 2 (1 req) | 0 | Required by 1 signal(s); removing it breaks materialization. |
| `secop_contract_execution` | promoted | keep | 2 (2 req) | 0 | Required by 2 signal(s); removing it breaks materialization. |
| `secop_contract_suspensions` | promoted | keep | 1 (0 req) | 0 | Promoted and visible in dependencies/output (1 signal refs, 0 evidence refs). |
| `secop_i_historical_processes` | promoted | keep | 0 (0 req) | 1098 | Promoted and visible in dependencies/output (0 signal refs, 1098 evidence refs). |
| `secop_ii_contracts` | promoted | keep | 20 (19 req) | 0 | Core source in bootstrap; it anchors contracts, sanctions, or public-official identity for many signals. |
| `secop_ii_processes` | promoted | keep | 4 (3 req) | 5 | Required by 3 signal(s); removing it breaks materialization. |
| `secop_integrado` | promoted | keep | 9 (7 req) | 2840 | Core source in bootstrap; it anchors contracts, sanctions, or public-official identity for many signals. |
| `secop_invoices` | promoted | keep | 1 (0 req) | 0 | Promoted and visible in dependencies/output (1 signal refs, 0 evidence refs). |
| `secop_offers` | promoted | keep | 3 (0 req) | 0 | Promoted and visible in dependencies/output (3 signal refs, 0 evidence refs). |
| `secop_process_bpin` | enrichment_only | keep | 5 (3 req) | 0 | Required by 3 signal(s); removing it breaks materialization. |
| `secop_sanctions` | promoted | keep | 1 (1 req) | 69 | Required by 1 signal(s); removing it breaks materialization. |
| `secop_suppliers` | promoted | keep | 5 (5 req) | 2 | Required by 5 signal(s); removing it breaks materialization. |
| `sgr_expense_execution` | promoted | keep | 1 (0 req) | 0 | Promoted and visible in dependencies/output (1 signal refs, 0 evidence refs). |
| `sgr_projects` | promoted | keep | 1 (0 req) | 0 | Promoted and visible in dependencies/output (1 signal refs, 0 evidence refs). |
| `sigep_public_servants` | promoted | keep | 2 (1 req) | 0 | Core source in bootstrap; it anchors contracts, sanctions, or public-official identity for many signals. |
| `sigep_sensitive_positions` | promoted | keep | 1 (0 req) | 0 | Promoted and visible in dependencies/output (1 signal refs, 0 evidence refs). |
| `siri_antecedents` | promoted | keep | 0 (0 req) | 34 | Promoted and visible in dependencies/output (0 signal refs, 34 evidence refs). |
| `dnp_project_contract_links` | enrichment_only | keep_context | 2 (0 req) | 0 | Used as optional/supporting data by 2 signal(s). |
| `dnp_project_locations` | enrichment_only | keep_context | 1 (0 req) | 0 | Used as optional/supporting data by 1 signal(s). |
| `higher_ed_directors` | enrichment_only | keep_context | 0 (0 req) | 8 | Appears in generated public case evidence (8 refs) even though signal deps do not list it. |
| `higher_ed_enrollment` | enrichment_only | keep_context | 0 (0 req) | 96 | Appears in generated public case evidence (96 refs) even though signal deps do not list it. |
| `higher_ed_institutions` | enrichment_only | keep_context | 0 (0 req) | 2 | Appears in generated public case evidence (2 refs) even though signal deps do not list it. |
| `igac_property_transactions` | enrichment_only | keep_context | 1 (0 req) | 0 | Used as optional/supporting data by 1 signal(s). |
| `secop_budget_commitments` | enrichment_only | keep_context | 1 (0 req) | 0 | Used as optional/supporting data by 1 signal(s). |
| `secop_cdp_requests` | enrichment_only | keep_context | 0 (0 req) | 27 | Appears in generated public case evidence (27 refs) even though signal deps do not list it. |
| `secop_contract_modifications` | enrichment_only | keep_context | 1 (0 req) | 0 | Used as optional/supporting data by 1 signal(s). |
| `secop_execution_locations` | enrichment_only | keep_context | 0 (0 req) | 4 | Appears in generated public case evidence (4 refs) even though signal deps do not list it. |
| `secop_payment_plans` | enrichment_only | keep_context | 1 (0 req) | 3 | Used as optional/supporting data by 1 signal(s). |
| `asset_disclosures` | promoted | keep_or_fix_deps | 0 (0 req) | 0 | Promoted in registry, but current signal deps/output do not clearly use it; either wire it to signals or demote it. |
| `health_providers` | promoted | keep_or_fix_deps | 0 (0 req) | 0 | Promoted in registry, but current signal deps/output do not clearly use it; either wire it to signals or demote it. |
| `secop_interadmin_agreements` | promoted | keep_or_fix_deps | 0 (0 req) | 0 | Promoted in registry, but current signal deps/output do not clearly use it; either wire it to signals or demote it. |
| `secop_budget_items` | quarantined | quarantine | 0 (0 req) | 0 | Explicitly quarantined in bootstrap; keep out of signal scoring until quality or join behavior is fixed. |
| `company_branches_nb3d` | enrichment_only | review_context | 0 (0 req) | 0 | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `dnp_project_beneficiary_characterization` | enrichment_only | review_context | 0 (0 req) | 0 | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `dnp_project_beneficiary_locations` | enrichment_only | review_context | 0 (0 req) | 0 | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `dnp_project_executors` | enrichment_only | review_context | 0 (0 req) | 0 | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `mapa_inversiones_projects` | enrichment_only | review_context | 0 (0 req) | 0 | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `pte_sector_commitments` | enrichment_only | review_context | 0 (0 req) | 0 | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `pte_top_contracts` | enrichment_only | review_context | 0 (0 req) | 0 | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `rues_chambers` | enrichment_only | review_context | 0 (0 req) | 0 | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `supersoc_top_companies` | enrichment_only | review_context | 0 (0 req) | 0 | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `secop_additional_locations` | enrichment_only | review_or_drop | 0 (0 req) | 0 | Implemented but not clearly used by current signals or visible outputs. |
| `secop_document_archives` | enrichment_only | review_or_drop | 0 (0 req) | 0 | Implemented but not clearly used by current signals or visible outputs. |
| `secop_i_resource_origins` | enrichment_only | review_or_drop | 0 (0 req) | 0 | Implemented but not clearly used by current signals or visible outputs. |

## Current questionable or cleanup candidates

| source_id | recommendation | why |
| --- | --- | --- |
| `registraduria_death_status_checks` | hold | External/manual access is blocked; not useful operationally until data can be lawfully and repeatably loaded. |
| `asset_disclosures` | keep_or_fix_deps | Promoted in registry, but current signal deps/output do not clearly use it; either wire it to signals or demote it. |
| `health_providers` | keep_or_fix_deps | Promoted in registry, but current signal deps/output do not clearly use it; either wire it to signals or demote it. |
| `secop_interadmin_agreements` | keep_or_fix_deps | Promoted in registry, but current signal deps/output do not clearly use it; either wire it to signals or demote it. |
| `secop_budget_items` | quarantine | Explicitly quarantined in bootstrap; keep out of signal scoring until quality or join behavior is fixed. |
| `company_branches_nb3d` | review_context | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `dnp_project_beneficiary_characterization` | review_context | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `dnp_project_beneficiary_locations` | review_context | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `dnp_project_executors` | review_context | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `mapa_inversiones_projects` | review_context | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `pte_sector_commitments` | review_context | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `pte_top_contracts` | review_context | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `rues_chambers` | review_context | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `supersoc_top_companies` | review_context | Useful enrichment category, but no direct current signal dependency/output evidence was found. |
| `secop_additional_locations` | review_or_drop | Implemented but not clearly used by current signals or visible outputs. |
| `secop_document_archives` | review_or_drop | Implemented but not clearly used by current signals or visible outputs. |
| `secop_i_resource_origins` | review_or_drop | Implemented but not clearly used by current signals or visible outputs. |

## Signal deps that point outside the current registry

These are not safe to delete blindly because signal SQL/dependency config references them, but they are not part of the 52-source registry/bootstrap universe.

| source_id | pipeline_key | signals | recommendation |
| --- | --- | --- | --- |
| `adverse_media` | `adverse_media` | 1 (1 req) | add_to_registry_or_remove_signal_ref |
| `dnp_obras_prioritarias` | `dnp_obras_prioritarias` | 2 (0 req) | add_to_registry_or_remove_signal_ref |
| `actos_administrativos` | `actos_administrativos` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `actos_administrativos` | `administrative_acts` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `anim_inmuebles` | `anim_inmuebles` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `anla_licencias` | `anla_concesiones` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `anm_titulos` | `anm_titulos_mineros` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `control_politico` | `control_politico` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `control_politico` | `control_politico_requirements` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `control_politico` | `control_politico_sessions` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `dane_ipm` | `dane_ipm_municipal` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `dane_micronegocios` | `dane_micronegocios` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `dane_pobreza_monetaria` | `dane_pobreza_monetaria` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `environmental_files_corantioquia` | `environmental_files` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `environmental_files_corantioquia` | `environmental_files_corantioquia` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `gacetas_territoriales` | `gacetas_territoriales` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `gacetas_territoriales` | `territorial_gazettes` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `igac_parcelas` | `igac_catastro_alfanumerico` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `judicial_providencias` | `judicial_cases` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `judicial_providencias` | `judicial_providencias` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `mindeporte_actores` | `mindeporte_actores` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `pdet_municipios` | `pdet_municipios` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `pnis_beneficiarios` | `pnis_beneficiarios` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `presidencia_iniciativas_33007` | `presidencia_iniciativas_33007` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `rub_beneficial_owners` | `rub_beneficial_owners` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `sirr_reincorporacion` | `sirr_reincorporacion` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `tvec_orders_consolidated` | `tvec_orders` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `tvec_orders_consolidated` | `tvec_orders_consolidated` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `ungrd_damnificados` | `ungrd_emergencias` | 1 (0 req) | add_to_registry_or_remove_signal_ref |
| `upme_subsidios` | `upme_subsidios_foes` | 1 (0 req) | add_to_registry_or_remove_signal_ref |

## Colombia open data audit summary

The audit file has `550` unique Socrata IDs: `285` valid, `208` dead/404, and `57` forbidden/403.
The audit output itself contains names/sectors only for valid IDs; 404/403 rows are ID-only inside each sector block, so relevance for those is necessarily "not usable now" until recovered.

### Audit recommendation counts

- `candidate`: 32
- `candidate_context`: 66
- `drop`: 346
- `hold`: 57
- `keep`: 18
- `manual_review`: 31

### Highest-value new audit candidates

| id | sector | name | why |
| --- | --- | --- | --- |
| `xjxk-qhsc` | Finance and Public Credit Sector Roadmap | Ejecución Presupuestal del Presupuesto General de la Nación | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `bpij-5vy9` | Finance and Public Credit Sector Roadmap | Ejecución Presupuestal del Presupuesto General de la Nación detallada por Rubro Presupuestal | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `5phs-yqfw` | Finance and Public Credit Sector Roadmap | Información de Gastos del Presupuesto General de la Nación | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `f4a5-ab9q` | Justice and Law Sector Roadmap | Seguimiento a la Ejecución Presupuestal del Sector Justicia | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `r29n-7bcm` | Labor | Sanciones Ejecutoriadas Y No Ejecutoriadas Por Intermediación Laboral Indebida por Dirección Territorial | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `bgt6-3icu` | Labor Sector Roadmap | Sanciones Ejecutoriadas y NO Ejecutoriadas por Conductas Atentatorias contra el derecho de asociación sindical | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `jh3r-qtfq` | Labor Sector Roadmap | Sanciones no Ejecutoriadas y Ejecutoriadas por Dirección Territorial | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `yn26-hitk` | Labor Sector Roadmap | Sanciones no Ejecutoriadas y Ejecutoriadas por sectores económicos | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `fs36-azrv` | MINCIT Sectoral Roadmap 2026 | Registro de Sanciones Contadores | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `ceth-n4bn` | National Planning Sector Roadmap | Grupos de Proveedores - SECOP II | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `4n4q-k399` | National Planning Sector Roadmap | Multas y Sanciones SECOP I | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `hgi6-6wh3` | National Planning Sector Roadmap | Proponentes por Proceso SECOP II | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `7fix-nd37` | National Planning Sector Roadmap | SECOP I - Adiciones | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `ps88-5e3v` | National Planning Sector Roadmap | SECOP I - Archivos Descarga | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `u5b4-ae3s` | National Planning Sector Roadmap | SECOP I - Modificaciones a Adjudicaciones | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `36vw-pbq2` | National Planning Sector Roadmap | SECOP I - Modificaciones a Procesos | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `azeg-sgqg` | National Planning Sector Roadmap | SECOP I - PAA Detalle | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `prdx-nxyp` | National Planning Sector Roadmap | SECOP I - PAA Encabezado | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `f789-7hwg` | National Planning Sector Roadmap | SECOP I - Procesos de Compra Pública | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `tauh-5jvn` | National Planning Sector Roadmap | SECOP I - Proponentes | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `4ex9-j3n8` | National Planning Sector Roadmap | SECOP II - Contacto Entidades y Proveedores | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `gjp9-cutm` | National Planning Sector Roadmap | SECOP II - Garantias | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `e2u2-swiw` | National Planning Sector Roadmap | SECOP II - Modificaciones a Procesos | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `b6m4-qgqv` | National Planning Sector Roadmap | SECOP II - PAA - Encabezado | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `9sue-ezhx` | National Planning Sector Roadmap | SECOPII - Plan Anual De Adquisiciones Detalle | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `3hdv-smhz` | National Planning Sector Roadmap | TVEC - Compras por item | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `usqp-5nsn` | National Planning Sector Roadmap | TVEC - Items | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `pqdu-ej7f` | Agriculture and Rural Development Sector Roadmap | Predios Beneficiarios PIDAR | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `br9a-gygu` | Finance and Public Credit Sector Roadmap | Ejecución Financiera de Regalías | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `h2yr-zfb2` | Habitat and Housing Sector Roadmap | Subsidios De Vivienda Asignados | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `xqsb-y246` | Justice and Law Sector Roadmap | USPEC Plan Anual de Adquisiciones | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |
| `rgxm-mmea` | National Planning Sector Roadmap | Tienda Virtual del Estado Colombiano - Consolidado | Name/sector points to procurement, budget, sanctions, public officials, suppliers, projects, or payments: directly relevant to corruption-pattern detection. |

## Bottom line

- Keep SECOP, sanctions, SIGEP/public-official identity, company identity, fiscal responsibility, Cuentas Claras, SGR/DNP BPIN, and official-case bulletin layers. Those are the corruption-pattern backbone.
- Demote or quarantine broad sector datasets that are not tied to entities, money, sanctions, contracts, territory, or named public cases.
- Do not ingest every valid audit dataset. Most valid audit rows are administrative inventories, education/labor statistics, foreign affairs, culture/sports, or generic publication indexes.
- Before adding any new audit candidate, run metadata/row-count triage and prove a join key: NIT/document id, process id, contract id, BPIN, municipality/divipola, or official entity id.

Full row-level appendix: `docs/datasets/dataset_relevance_appendix.csv`
