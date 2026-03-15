# Pipeline Status

Generated from `docs/source_registry_co_v1.csv` (as-of UTC: 2026-03-14T15:35:15Z).

Status buckets:
- `implemented_loaded`: implemented and loaded in registry.
- `implemented_partial`: implemented but partial/stale/not fully loaded.
- `blocked_external`: implemented but externally blocked.
- `not_built`: not implemented in public repo.

| Source ID | Pipeline ID | Status Bucket | Load State | Source Format | Required Input | Known Blockers |
|---|---|---|---|---|---|---|
| asset_disclosures | asset_disclosures | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/8tz7-h3eu | Socrata dataset `8tz7-h3eu`. Ley 2013 de 2019 asset and income disclosures for public servants and contractors. |
| conflict_disclosures | conflict_disclosures | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/gbry-rnq4 | Socrata dataset `gbry-rnq4`. Ley 2013 de 2019 conflict-of-interest disclosures. |
| cuentas_claras_income_2019 | cuentas_claras_income_2019 | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/jgra-rz2t | Socrata dataset `jgra-rz2t`. Campaign income reported to Cuentas Claras for 2019 local elections. |
| health_providers | health_providers | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/c36g-9fc2 | Socrata dataset `c36g-9fc2`. REPS health providers and service sites. |
| higher_ed_enrollment | higher_ed_enrollment | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/5wck-szir | Socrata dataset `5wck-szir`. Higher-education enrollment statistics from MEN. |
| igac_property_transactions | igac_property_transactions | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/7y2j-43cv | Socrata dataset `7y2j-43cv`. Property transactions from the Observatorio Inmobiliario Catastral; ingested as aggregated market-activity signals. |
| mapa_inversiones_projects | mapa_inversiones_projects | implemented_partial | not_loaded | api_json | API payload from https://mapainversiones.dnp.gov.co/reportes/datosbasicos | MapaInversiones report API for project basics and execution-stage budget context. |
| paco_sanctions | paco_sanctions | implemented_partial | not_loaded | web_portal | Portal export/scrape output under data/paco_sanctions/ | PACO portal feeds for fiscal responsibility, procurement collusion, disciplinary sanctions, and SECOP fines. |
| pte_sector_commitments | pte_sector_commitments | implemented_partial | not_loaded | web_portal | Portal export/scrape output under data/pte_sector_commitments/ | PTE CSV export for current-year PGN commitments aggregated by sector. |
| pte_top_contracts | pte_top_contracts | implemented_partial | not_loaded | web_portal | Portal export/scrape output under data/pte_top_contracts/ | PTE CSV export for the largest current-year PGN contracts and beneficiary/payment context. |
| registraduria_death_status_checks | registraduria_death_status_checks | blocked_external | not_loaded | unknown | source-specific contract required | Normalized manual imports of Registraduria document-status consultations for death-status and identity-vigency screening. |
| rues_chambers | rues_chambers | implemented_partial | not_loaded | web_portal | Portal export/scrape output under data/rues_chambers/ | Public chamber directory from RUES elastic endpoints combining chamber catalog and chamber detail records. |
| secop_budget_commitments | secop_budget_commitments | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/skc9-met7 | Socrata dataset `skc9-met7`. SECOP II budget commitments and SIIF-linked commitment balances for contracts. |
| secop_cdp_requests | secop_cdp_requests | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/a86w-fh92 | Socrata dataset `a86w-fh92`. SECOP II CDP request balances, funding sources, and SIIF validation metadata. |
| secop_contract_additions | secop_contract_additions | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/cb9c-h8sn | Socrata dataset `cb9c-h8sn`. Contract additions recorded in SECOP II. |
| secop_contract_execution | secop_contract_execution | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/mfmm-jqmq | Socrata dataset `mfmm-jqmq`. Execution progress and delivery performance for SECOP II contracts. |
| secop_contract_modifications | secop_contract_modifications | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/u8cx-r425 | Socrata dataset `u8cx-r425`. Formal contract modifications, extensions, and value changes in SECOP II. |
| secop_execution_locations | secop_execution_locations | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/gra4-pcp2 | Socrata dataset `gra4-pcp2`. Execution locations published for SECOP II contracts. |
| secop_ii_contracts | secop_ii_contracts | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/jbjy-vk9h | Socrata dataset `jbjy-vk9h`. Electronic contracts from SECOP II, including supplier and entity funding fields. |
| secop_ii_processes | secop_ii_processes | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/p6dx-8zbt | Socrata dataset `p6dx-8zbt`. SECOP II procurement procedures and award outcomes. |
| secop_integrado | secop_integrado | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/rpmr-utcd | Socrata dataset `rpmr-utcd`. Integrated SECOP I/II contract awards with contractor document identifiers. |
| secop_invoices | secop_invoices | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/ibyt-yi2f | Socrata dataset `ibyt-yi2f`. SECOP II invoice, delivery, and expected payment records for contracts. |
| secop_offers | secop_offers | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/wi7w-2nvm | Socrata dataset `wi7w-2nvm`. Offer-level bidder participation and submitted values for SECOP II processes. |
| secop_sanctions | secop_sanctions | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/it5q-hg94 | Socrata dataset `it5q-hg94`. SECOP II fines and sanctions tied to suppliers and contracts. |
| secop_suppliers | secop_suppliers | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/qmzu-gj57 | Socrata dataset `qmzu-gj57`. Supplier registry for SECOP II with legal representative metadata. |
| sgr_expense_execution | sgr_expense_execution | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/qkv4-ek54 | Socrata dataset `qkv4-ek54`. Execution of spending for the Sistema General de Regalías. |
| sgr_projects | sgr_projects | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/mzgh-shtp | Socrata dataset `mzgh-shtp`. Public investment projects financed by the royalty system. |
| sigep_public_servants | sigep_public_servants | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/2jzx-383z | Socrata dataset `2jzx-383z`. Active public servants registered in SIGEP. |
| sigep_sensitive_positions | sigep_sensitive_positions | implemented_partial | not_loaded | api_json | API payload from https://www.datos.gov.co/d/5u9e-g5w9 | Socrata dataset `5u9e-g5w9`. SIGEP positions with elevated corruption or budget-control exposure. |
| supersoc_top_companies | supersoc_top_companies | implemented_partial | not_loaded | web_portal | Portal export/scrape output under data/supersoc_top_companies/ | Supersociedades annual top-company workbook normalized to CSV and linked to company financial-capacity records. |
