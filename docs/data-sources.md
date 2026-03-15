# Colombia Data Source Catalog

This catalog defines the Colombia-only public source universe for `co/acc`.

<!-- SOURCE_SUMMARY_START -->
**Generated from `docs/source_registry_co_v1.csv` (as-of UTC: 2026-03-14T15:35:15Z)**

- Universe v1 sources: 30
- Implemented pipelines: 30
- Loaded sources (load_state=loaded): 0
- Partial sources (load_state=partial): 0
- Not loaded sources (load_state=not_loaded): 30
- Status counts: loaded=0, partial=29, stale=0, blocked_external=1, not_built=0
<!-- SOURCE_SUMMARY_END -->

## Implemented In Public Repo

<!-- IMPLEMENTED_SOURCES_START -->
| Source | Pipeline | What it loads | Notes |
|---|---|---|---|
| Declaración de activos - patrimonial servidores públicos | `asset_disclosures` | `Person`, `DeclaredAsset`, `DECLAROU_BEM` | Ley 2013 asset disclosure summaries for public servants and contractors. |
| Declaraciones conflictos de interés | `conflict_disclosures` | `Person`, `Finance`, `DECLAROU_FINANCA` | Ley 2013 conflict-of-interest disclosures normalized as finance records. |
| Base ingresos cuentas claras 2019 | `cuentas_claras_income_2019` | `Person`, `Company`, `Election`, `CANDIDATO_EM`, `DOOU` | Campaign income disclosures for the 2019 territorial election cycle. |
| Registro Especial de Prestadores y Sedes de Servicios de Salud | `health_providers` | `Company`, `Health`, `OPERA_UNIDADE` | REPS provider organizations and their health-service sites. |
| MEN_MATRICULA_ESTADISTICA_ES | `higher_ed_enrollment` | `Company`, `Education`, `MANTEDORA_DE` | MEN enrollment aggregates by institution, program, year, and semester. |
| IGAC OIC - transacciones inmobiliarias | `igac_property_transactions` | `Company`, `Finance`, `ADMINISTRA` | Municipality-level IGAC property-market activity aggregated from transaction rows. |
| MapaInversiones - reporte proyectos datos básicos | `mapa_inversiones_projects` | `Company`, `Convenio`, `ADMINISTRA` | MapaInversiones project basics tied to responsible public entities. |
| PACO - sanciones y red flags | `paco_sanctions` | `Company`, `Person`, `Sanction`, `SANCIONADA` | PACO fiscal, disciplinary, procurement-collusion, and SECOP-fine feeds. |
| PTE - compromisos por sector | `pte_sector_commitments` | `Finance` | Current-year PGN sector commitment aggregates exported from PTE. |
| PTE - 100 contratos más grandes | `pte_top_contracts` | `Company`, `Finance`, `ADMINISTRA`, `BENEFICIOU`, `REFERENTE_A` | Top current-year PGN contracts and beneficiaries exported from PTE. |
| Registraduria - checks de defuncion y vigencia | `registraduria_death_status_checks` | `Person` | Manual-imported Registraduria status checks for identity vigency and death-status screening. |
| RUES - camaras de comercio | `rues_chambers` | `Company` | Public RUES chamber directory merged from chamber list and detailed chamber metadata endpoints. |
| SECOP II - Compromisos Presupuestales | `secop_budget_commitments` | `Contract` | Aggregated SECOP II budget commitment balances and SIIF-linked commitment metadata. |
| SECOP II - Solicitudes CDPs | `secop_cdp_requests` | `Contract` | CDP request balances, funding-source totals, and SIIF validation metadata merged into contracts. |
| SECOP II - Adiciones | `secop_contract_additions` | `Contract` | Aggregated additions metadata merged back into SECOP II contract nodes. |
| SECOP II - Ejecución Contratos | `secop_contract_execution` | `Contract` | Aggregated execution progress and milestone metrics merged into contracts. |
| SECOP II - Modificaciones a contratos | `secop_contract_modifications` | `Contract` | Aggregated modification and value-change metadata merged into contracts. |
| SECOP II - Ubicaciones ejecucion contratos | `secop_execution_locations` | `Contract` | Execution-location points aggregated back into SECOP II contract relationships. |
| SECOP II - Contratos Electrónicos | `secop_ii_contracts` | `Company`, `Person`, `Contract`, `VENCEU`, `OFFICER_OF`, `REFERENTE_A` | Electronic SECOP II contracts linked back to procurement portfolio records. |
| SECOP II - Procesos de Contratación | `secop_ii_processes` | `Company`, `ADJUDICOU_A` | SECOP II procurement procedures normalized into buyer-to-awarded-supplier summaries. |
| SECOP Integrado | `secop_integrado` | `Company`, `Contract`, `VENCEU` | Integrated SECOP I/II contract awards using contractor document identifiers. |
| SECOP II - Facturas | `secop_invoices` | `Contract` | Invoice totals, delivery dates, and payment expectations merged into contracts. |
| SECOP II - Ofertas Por Proceso | `secop_offers` | `Company`, `Bid`, `LICITOU`, `FORNECEU_LICITACAO` | Offer-level bidder participation and submitted values for SECOP II processes. |
| SECOPII - Multas y Sanciones | `secop_sanctions` | `Company`, `Sanction`, `SANCIONADA` | Combined SECOP I and SECOP II sanctions feeds. |
| SECOP II - Proveedores Registrados | `secop_suppliers` | `Company`, `Person`, `OFFICER_OF` | Supplier registry and legal representative metadata from SECOP II. |
| OVCF - SGR - Ejecución de Gastos | `sgr_expense_execution` | `Company`, `Finance`, `FORNECEU` | SGR expense execution rows linked to registered third parties. |
| DNP-ProyectosSGR | `sgr_projects` | `Company`, `Convenio`, `ADMINISTRA` | Royalty-system investment projects tied to their executing entities. |
| Conjunto servidores públicos | `sigep_public_servants` | `Person`, `PublicOffice`, `RECEBEU_SALARIO` | Current SIGEP public-servant positions with office and salary metadata. |
| Puestos Sensibles a la Corrupción | `sigep_sensitive_positions` | `Person`, `PublicOffice`, `RECEBEU_SALARIO` | SIGEP sensitive-position subset with integrity-risk flags on offices and relationships. |
| Supersociedades - 1000 empresas más grandes | `supersoc_top_companies` | `Company`, `Finance`, `DECLAROU_FINANCA` | Supersociedades top-company filings with revenue, assets, liabilities, and profit metrics. |
<!-- IMPLEMENTED_SOURCES_END -->

## Priority Official Colombia Sources

| Source ID | Dataset | Category | URL |
|---|---|---|---|
| `secop_offers` | SECOP II - Ofertas Por Proceso | Contracts | https://www.datos.gov.co/d/wi7w-2nvm |
| `secop_ii_processes` | SECOP II - Procesos de Contratación | Contracts | https://www.datos.gov.co/d/p6dx-8zbt |
| `secop_ii_contracts` | SECOP II - Contratos Electrónicos | Contracts | https://www.datos.gov.co/d/jbjy-vk9h |
| `secop_budget_commitments` | SECOP II - Compromisos Presupuestales | Contracts | https://www.datos.gov.co/d/skc9-met7 |
| `secop_cdp_requests` | SECOP II - Solicitudes CDPs | Contracts | https://www.datos.gov.co/d/a86w-fh92 |
| `secop_invoices` | SECOP II - Facturas | Contracts | https://www.datos.gov.co/d/ibyt-yi2f |
| `secop_execution_locations` | SECOP II - Ubicaciones ejecucion contratos | Contracts | https://www.datos.gov.co/d/gra4-pcp2 |
| `secop_suppliers` | SECOP II - Proveedores Registrados | Identity | https://www.datos.gov.co/d/qmzu-gj57 |
| `sigep_public_servants` | Conjunto servidores públicos | Public sector | https://www.datos.gov.co/d/2jzx-383z |
| `sigep_sensitive_positions` | Puestos Sensibles a la Corrupción | Public sector | https://www.datos.gov.co/d/5u9e-g5w9 |
| `asset_disclosures` | Declaración de activos - patrimonial servidores públicos | Disclosures | https://www.datos.gov.co/d/8tz7-h3eu |
| `conflict_disclosures` | Declaraciones conflictos de interés | Disclosures | https://www.datos.gov.co/d/gbry-rnq4 |
| `sgr_expense_execution` | OVCF - SGR - Ejecución de Gastos | Budget | https://www.datos.gov.co/d/qkv4-ek54 |
| `sgr_projects` | DNP-ProyectosSGR | Budget | https://www.datos.gov.co/d/mzgh-shtp |
| `health_providers` | Registro Especial de Prestadores y Sedes de Servicios de Salud | Health | https://www.datos.gov.co/d/c36g-9fc2 |
| `higher_ed_enrollment` | MEN_MATRICULA_ESTADISTICA_ES | Education | https://www.datos.gov.co/d/5wck-szir |
| `cuentas_claras_income_2019` | Base ingresos cuentas claras 2019 | Electoral | https://www.datos.gov.co/d/jgra-rz2t |

## Modeling Notes

- Colombia currently has no equivalent in this repo to Brazil's CNPJ-first backbone. The procurement-first graph therefore anchors companies on the document identifiers published in SECOP.
- Person-facing datasets exist in the registry, but public mode keeps person access disabled by default until disclosure-specific masking rules are tuned for Colombia.
- The registry is regenerated from official Socrata catalog metadata with `scripts/sync_colombia_portal_registry.py`.
