# Pipeline Status

Generated from `docs/source_registry_co_v1.csv` + live runtime overlay from `http://localhost:8000/api/v1/meta/sources` (as-of UTC: 2026-03-27T19:51:51Z).

Status buckets:
- `implemented_loaded`: implemented and loaded in registry.
- `implemented_partial`: implemented but partial/stale/not fully loaded.
- `blocked_external`: implemented but externally blocked.
- `not_built`: not implemented in public repo.
- Signal roles: `promoted` drives user-facing signals, `enrichment_only` is supporting evidence, `quarantined` is excluded from signal generation.

| Source ID | Pipeline ID | Status Bucket | Signal Role | Load State | Source Format | Required Input | Known Blockers |
|---|---|---|---|---|---|---|---|
| asset_disclosures | asset_disclosures | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/8tz7-h3eu | - |
| company_branches_nb3d | company_branches_nb3d | implemented_loaded | enrichment_only | loaded | unknown | source-specific contract required | - |
| company_registry_c82u | company_registry_c82u | implemented_loaded | promoted | loaded | unknown | source-specific contract required | - |
| conflict_disclosures | conflict_disclosures | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/gbry-rnq4 | - |
| cuentas_claras_income_2019 | cuentas_claras_income_2019 | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/jgra-rz2t | - |
| dnp_project_beneficiary_characterization | dnp_project_beneficiary_characterization | implemented_loaded | enrichment_only | loaded | unknown | source-specific contract required | - |
| dnp_project_beneficiary_locations | dnp_project_beneficiary_locations | implemented_loaded | enrichment_only | loaded | unknown | source-specific contract required | - |
| dnp_project_contract_links | dnp_project_contract_links | implemented_loaded | enrichment_only | loaded | unknown | source-specific contract required | - |
| dnp_project_executors | dnp_project_executors | implemented_loaded | enrichment_only | loaded | unknown | source-specific contract required | - |
| dnp_project_locations | dnp_project_locations | implemented_loaded | enrichment_only | loaded | unknown | source-specific contract required | - |
| fiscal_findings | fiscal_findings | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/8qxx-ubmq | - |
| fiscal_responsibility | fiscal_responsibility | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/jr8e-e8tu | - |
| health_providers | health_providers | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/c36g-9fc2 | - |
| higher_ed_directors | higher_ed_directors | implemented_loaded | enrichment_only | loaded | api_json | API payload from https://www.datos.gov.co/d/muyy-6yw9 | - |
| higher_ed_enrollment | higher_ed_enrollment | implemented_loaded | enrichment_only | loaded | api_json | API payload from https://www.datos.gov.co/d/5wck-szir | - |
| higher_ed_institutions | higher_ed_institutions | implemented_loaded | enrichment_only | loaded | api_json | API payload from https://www.datos.gov.co/d/n5yy-8nav | - |
| igac_property_transactions | igac_property_transactions | implemented_loaded | enrichment_only | loaded | api_json | API payload from https://www.datos.gov.co/d/7y2j-43cv | - |
| mapa_inversiones_projects | mapa_inversiones_projects | implemented_loaded | enrichment_only | loaded | api_json | API payload from https://mapainversiones.dnp.gov.co/reportes/datosbasicos | - |
| official_case_bulletins | official_case_bulletins | implemented_loaded | promoted | loaded | unknown | source-specific contract required | - |
| paco_sanctions | paco_sanctions | implemented_loaded | promoted | loaded | web_portal | Portal export/scrape output under data/paco_sanctions/ | - |
| pte_sector_commitments | pte_sector_commitments | implemented_loaded | enrichment_only | loaded | web_portal | Portal export/scrape output under data/pte_sector_commitments/ | - |
| pte_top_contracts | pte_top_contracts | implemented_loaded | enrichment_only | loaded | web_portal | Portal export/scrape output under data/pte_top_contracts/ | - |
| registraduria_death_status_checks | registraduria_death_status_checks | blocked_external | enrichment_only | not_loaded | unknown | source-specific contract required | Normalized manual imports of Registraduria document-status consultations for death-status and identity-vigency screening. |
| rues_chambers | rues_chambers | implemented_loaded | enrichment_only | loaded | web_portal | Portal export/scrape output under data/rues_chambers/ | - |
| secop_additional_locations | secop_additional_locations | implemented_loaded | enrichment_only | loaded | unknown | source-specific contract required | - |
| secop_budget_commitments | secop_budget_commitments | implemented_loaded | enrichment_only | loaded | api_json | API payload from https://www.datos.gov.co/d/skc9-met7 | - |
| secop_budget_items | secop_budget_items | implemented_loaded | quarantined | loaded | api_json | API payload from https://www.datos.gov.co/d/cwhv-7fnp | - |
| secop_cdp_requests | secop_cdp_requests | implemented_loaded | enrichment_only | loaded | api_json | API payload from https://www.datos.gov.co/d/a86w-fh92 | - |
| secop_contract_additions | secop_contract_additions | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/cb9c-h8sn | - |
| secop_contract_execution | secop_contract_execution | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/mfmm-jqmq | - |
| secop_contract_modifications | secop_contract_modifications | implemented_loaded | enrichment_only | loaded | api_json | API payload from https://www.datos.gov.co/d/u8cx-r425 | - |
| secop_contract_suspensions | secop_contract_suspensions | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/u99c-7mfm | - |
| secop_document_archives | secop_document_archives | implemented_loaded | enrichment_only | loaded | unknown | source-specific contract required | - |
| secop_execution_locations | secop_execution_locations | implemented_loaded | enrichment_only | loaded | api_json | API payload from https://www.datos.gov.co/d/gra4-pcp2 | - |
| secop_i_historical_processes | secop_i_historical_processes | implemented_loaded | promoted | loaded | unknown | source-specific contract required | - |
| secop_i_resource_origins | secop_i_resource_origins | implemented_loaded | enrichment_only | loaded | unknown | source-specific contract required | - |
| secop_ii_contracts | secop_ii_contracts | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/jbjy-vk9h | - |
| secop_ii_processes | secop_ii_processes | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/p6dx-8zbt | - |
| secop_integrado | secop_integrado | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/rpmr-utcd | - |
| secop_interadmin_agreements | secop_interadmin_agreements | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/s484-c9k3 | - |
| secop_invoices | secop_invoices | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/ibyt-yi2f | - |
| secop_offers | secop_offers | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/wi7w-2nvm | - |
| secop_payment_plans | secop_payment_plans | implemented_loaded | enrichment_only | loaded | api_json | API payload from https://www.datos.gov.co/d/uymx-8p3j | - |
| secop_process_bpin | secop_process_bpin | implemented_loaded | enrichment_only | loaded | api_json | API payload from https://www.datos.gov.co/d/d9na-abhe | - |
| secop_sanctions | secop_sanctions | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/it5q-hg94 | - |
| secop_suppliers | secop_suppliers | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/qmzu-gj57 | - |
| sgr_expense_execution | sgr_expense_execution | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/qkv4-ek54 | - |
| sgr_projects | sgr_projects | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/mzgh-shtp | - |
| sigep_public_servants | sigep_public_servants | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/2jzx-383z | - |
| sigep_sensitive_positions | sigep_sensitive_positions | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/5u9e-g5w9 | - |
| siri_antecedents | siri_antecedents | implemented_loaded | promoted | loaded | api_json | API payload from https://www.datos.gov.co/d/iaeu-rcn6 | - |
| supersoc_top_companies | supersoc_top_companies | implemented_loaded | enrichment_only | loaded | web_portal | Portal export/scrape output under data/supersoc_top_companies/ | - |
