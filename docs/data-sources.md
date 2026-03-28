# Colombia Data Source Catalog

This catalog defines the Colombia-only public source universe for `co/acc`.

<!-- SOURCE_SUMMARY_START -->
**Generated from `docs/source_registry_co_v1.csv` + live runtime overlay from `http://localhost:8000/api/v1/meta/sources` (as-of UTC: 2026-03-27T19:51:52Z)**

- Universe v1 sources: 52
- Implemented pipelines: 52
- Loaded sources (load_state=loaded): 51
- Partial sources (load_state=partial): 0
- Not loaded sources (load_state=not_loaded): 1
- Signal roles: promoted=26, enrichment_only=25, quarantined=1
- Status counts: loaded=51, partial=0, stale=0, blocked_external=1, not_built=0
<!-- SOURCE_SUMMARY_END -->

## Implemented In Public Repo

<!-- IMPLEMENTED_SOURCES_START -->
| Source | Pipeline | Signal Role | What it loads | Notes |
|---|---|---|---|---|
| Declaración de activos - patrimonial servidores públicos | `asset_disclosures` | `promoted` | `Person`, `DeclaredAsset`, `DECLARO_BIEN` | Ley 2013 asset disclosure summaries for public servants and contractors. |
| RUES - establecimientos y sucursales conectadas | `company_branches_nb3d` | `enrichment_only` | See pipeline implementation. | Socrata dataset `nb3d-v3n7`. Connected-batch establishment and branch rows linked only when the owner document already exists in the live graph. |
| Personas Naturales Personas Jurídicas y Entidades Sin Ánimo de Lucro | `company_registry_c82u` | `promoted` | See pipeline implementation. | Socrata dataset `c82u-588k`. Connected-batch chamber registry layer for companies and legal representatives that now produces exact document officer overlaps with contracted suppliers and public servants. |
| Declaraciones conflictos de interés | `conflict_disclosures` | `promoted` | `Person`, `Finance`, `DECLARO_FINANZAS` | Ley 2013 conflict-of-interest disclosures normalized as finance records. |
| Base ingresos cuentas claras 2019 | `cuentas_claras_income_2019` | `promoted` | `Person`, `Company`, `Election`, `CANDIDATO_EM`, `DONO_A` | Campaign income disclosures for the 2019 territorial election cycle. |
| DNP - caracterización demográfica de beneficiarios | `dnp_project_beneficiary_characterization` | `enrichment_only` | See pipeline implementation. | Socrata dataset `tmmn-mpqc`. Connected-batch DNP beneficiary demographic breakdown by BPIN |
| DNP - localización de beneficiarios | `dnp_project_beneficiary_locations` | `enrichment_only` | See pipeline implementation. | Socrata dataset `iuc2-3r6h`. Connected-batch DNP beneficiary geography rows keyed by BPIN and responsible entity. |
| DNP - enlaces BPIN a contratos | `dnp_project_contract_links` | `enrichment_only` | See pipeline implementation. | Derived graph link layer joining BPIN-coded procurement exposure onto Convenio project nodes without inventing contract facts. |
| DNP - ejecutores de proyectos | `dnp_project_executors` | `enrichment_only` | See pipeline implementation. | Socrata dataset `epzv-8ck4`. Connected-batch DNP executor rows keyed by BPIN and executing entity. |
| DNP - localización de proyectos | `dnp_project_locations` | `enrichment_only` | See pipeline implementation. | Socrata dataset `xikz-44ja`. Connected-batch DNP geographic footprint for projects and responsible entities keyed by BPIN. |
| Hallazgos Fiscales | `fiscal_findings` | `promoted` | `Company`, `Finding`, `TIENE_HALLAZGO` | Official Contraloría fiscal findings tied to audited entities and radicados. |
| Responsabilidad Fiscal | `fiscal_responsibility` | `promoted` | `Company`, `Sanction`, `SANCIONADA` | Official fiscal-responsibility sanctions from Contraloría / SIREF. |
| Registro Especial de Prestadores y Sedes de Servicios de Salud | `health_providers` | `promoted` | `Company`, `Health`, `OPERA_UNIDAD` | REPS provider organizations and their health-service sites. |
| MEN_DIRECTIVOS_DE_INSTITUCIONES_DE_EDUCACIÓN_SUPERIOR | `higher_ed_directors` | `enrichment_only` | See pipeline implementation. | Socrata dataset `muyy-6yw9`. Directors |
| MEN_MATRICULA_ESTADISTICA_ES | `higher_ed_enrollment` | `enrichment_only` | `Company`, `Education`, `MANTIENE_A` | MEN enrollment aggregates by institution, program, year, and semester. |
| MEN_INSTITUCIONES EDUCACIÓN SUPERIOR | `higher_ed_institutions` | `enrichment_only` | See pipeline implementation. | Socrata dataset `n5yy-8nav`. Official registry of higher-education institutions with institutional code and identification number. |
| IGAC OIC - transacciones inmobiliarias | `igac_property_transactions` | `enrichment_only` | `Company`, `Finance`, `ADMINISTRA` | Municipality-level IGAC property-market activity aggregated from transaction rows. |
| MapaInversiones - reporte proyectos datos básicos | `mapa_inversiones_projects` | `enrichment_only` | `Company`, `Convenio`, `ADMINISTRA` | MapaInversiones project basics tied to responsible public entities. |
| Boletines oficiales de casos priorizados | `official_case_bulletins` | `promoted` | See pipeline implementation. | Curated official Procuraduria and MEN bulletin layer normalized as Inquiry nodes and linked only through exact document ids or clearly flagged probable name matches. |
| PACO - sanciones y red flags | `paco_sanctions` | `promoted` | `Company`, `Person`, `Sanction`, `SANCIONADA` | PACO fiscal, disciplinary, procurement-collusion, and SECOP-fine feeds. |
| PTE - compromisos por sector | `pte_sector_commitments` | `enrichment_only` | `Finance` | Current-year PGN sector commitment aggregates exported from PTE. |
| PTE - 100 contratos más grandes | `pte_top_contracts` | `enrichment_only` | `Company`, `Finance`, `ADMINISTRA`, `BENEFICIO`, `REFERENTE_A` | Top current-year PGN contracts and beneficiaries exported from PTE. |
| Registraduria - checks de defuncion y vigencia | `registraduria_death_status_checks` | `enrichment_only` | `Person` | Manual-imported Registraduria status checks for identity vigency and death-status screening. |
| RUES - camaras de comercio | `rues_chambers` | `enrichment_only` | `Company` | Public RUES chamber directory merged from chamber list and detailed chamber metadata endpoints. |
| SECOP II - Ubicaciones Adicionales | `secop_additional_locations` | `enrichment_only` | See pipeline implementation. | Socrata dataset `wwhe-4sq8`. Connected-batch companion for contract addresses and additional execution locations published in SECOP II. |
| SECOP II - Compromisos Presupuestales | `secop_budget_commitments` | `enrichment_only` | `Contract` | Aggregated SECOP II budget commitment balances and SIIF-linked commitment metadata. |
| SECOP II - Rubros Presupuestales | `secop_budget_items` | `quarantined` | See pipeline implementation. | Socrata dataset `cwhv-7fnp`. Budget-item and commitment identifiers linked to SECOP II contracts. |
| SECOP II - Solicitudes CDPs | `secop_cdp_requests` | `enrichment_only` | `Contract` | CDP request balances, funding-source totals, and SIIF validation metadata merged into contracts. |
| SECOP II - Adiciones | `secop_contract_additions` | `promoted` | `Contract` | Aggregated additions metadata merged back into SECOP II contract nodes. |
| SECOP II - Ejecución Contratos | `secop_contract_execution` | `promoted` | `Contract` | Aggregated execution progress and milestone metrics merged into contracts. |
| SECOP II - Modificaciones a contratos | `secop_contract_modifications` | `enrichment_only` | `Contract` | Aggregated modification and value-change metadata merged into contracts. |
| SECOP II - Suspensiones de Contratos | `secop_contract_suspensions` | `promoted` | See pipeline implementation. | Socrata dataset `u99c-7mfm`. Contract suspension events in SECOP II, including approval dates and modification purposes. |
| SECOP II - Archivos Descarga Desde 2025 | `secop_document_archives` | `enrichment_only` | See pipeline implementation. | Socrata dataset `dmgg-8hin`. Connected-batch SECOP II archive index with downloadable document refs |
| SECOP II - Ubicaciones ejecucion contratos | `secop_execution_locations` | `enrichment_only` | `Contract` | Execution-location points aggregated back into SECOP II contract relationships. |
| SECOP I - Procesos de Compra Pública Histórico | `secop_i_historical_processes` | `promoted` | See pipeline implementation. | Socrata dataset `qddk-cgux`. Connected-batch historical SECOP I awarded processes with contractor and legal-representative identifiers |
| SECOP I - Origen de los Recursos | `secop_i_resource_origins` | `enrichment_only` | See pipeline implementation. | Socrata dataset `3xwx-53wt`. Resource-origin and BPIN companion for SECOP I adjudications linked onto historical award summaries. |
| SECOP II - Contratos Electrónicos | `secop_ii_contracts` | `promoted` | `Company`, `Person`, `Contract`, `GANO`, `OFFICER_OF`, `REFERENTE_A` | Electronic SECOP II contracts linked back to procurement portfolio records. |
| SECOP II - Procesos de Contratación | `secop_ii_processes` | `promoted` | `Company`, `ADJUDICOU_A` | SECOP II procurement procedures normalized into buyer-to-awarded-supplier summaries. |
| SECOP Integrado | `secop_integrado` | `promoted` | `Company`, `Contract`, `GANO` | Integrated SECOP I/II contract awards using contractor document identifiers. |
| SECOP - Convenios Interadministrativos | `secop_interadmin_agreements` | `promoted` | See pipeline implementation. | Socrata datasets `s484-c9k3` and `ityv-bxct`. Current and historical SECOP interadministrative agreements, with buyer, contractor, territory, and resource-origin fields. |
| SECOP II - Facturas | `secop_invoices` | `promoted` | `Contract` | Invoice totals, delivery dates, and payment expectations merged into contracts. |
| SECOP II - Ofertas Por Proceso | `secop_offers` | `promoted` | `Company`, `Bid`, `LICITO`, `SUMINISTRO_LICITACAO` | Offer-level bidder participation and submitted values for SECOP II processes. |
| SECOP II - Plan de pagos | `secop_payment_plans` | `enrichment_only` | See pipeline implementation. | Socrata dataset `uymx-8p3j`. Payment-plan, supervisor, CUFE, radicado, and actual-payment records tied to SECOP II contracts. |
| SECOP II - BPIN por Proceso | `secop_process_bpin` | `enrichment_only` | See pipeline implementation. | Socrata dataset `d9na-abhe`. BPIN identifiers and validation status attached to SECOP II processes and contracts. |
| SECOPII - Multas y Sanciones | `secop_sanctions` | `promoted` | `Company`, `Sanction`, `SANCIONADA` | Combined SECOP I and SECOP II sanctions feeds. |
| SECOP II - Proveedores Registrados | `secop_suppliers` | `promoted` | `Company`, `Person`, `OFFICER_OF` | Supplier registry and legal representative metadata from SECOP II. |
| OVCF - SGR - Ejecución de Gastos | `sgr_expense_execution` | `promoted` | `Company`, `Finance`, `SUMINISTRO` | SGR expense execution rows linked to registered third parties. |
| DNP-ProyectosSGR | `sgr_projects` | `promoted` | `Company`, `Convenio`, `ADMINISTRA` | Royalty-system investment projects tied to their executing entities. |
| Conjunto servidores públicos | `sigep_public_servants` | `promoted` | `Person`, `PublicOffice`, `RECIBIO_SALARIO` | Current SIGEP public-servant positions with office and salary metadata. |
| Puestos Sensibles a la Corrupción | `sigep_sensitive_positions` | `promoted` | `Person`, `PublicOffice`, `RECIBIO_SALARIO` | SIGEP sensitive-position subset with integrity-risk flags on offices and relationships. |
| Antecedentes de SIRI | `siri_antecedents` | `promoted` | `Person`, `Sanction`, `SANCIONADA` | Official SIRI antecedents with person-level disciplinary and related inhabilidad records. |
| Supersociedades - 1000 empresas más grandes | `supersoc_top_companies` | `enrichment_only` | `Company`, `Finance`, `DECLARO_FINANZAS` | Supersociedades top-company filings with revenue, assets, liabilities, and profit metrics. |
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

- The procurement-first graph anchors companies on the document identifiers (NIT) published in SECOP.
- Person-facing datasets exist in the registry, but public mode keeps person access disabled by default until disclosure-specific masking rules are tuned for Colombia.
- The registry is regenerated from official Socrata catalog metadata with `scripts/sync_colombia_portal_registry.py`.
