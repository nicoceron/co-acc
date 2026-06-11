# Missing corruption pattern detector blueprints

Generated: 2026-06-05

Purpose: implementation-ready detector cards derived from `high_confidence_missing_corruption_patterns_2026-06-05.md`. These are not allegations. They specify reviewer queues, evidence requirements, and false-positive gates for the highest-confidence missing corruption patterns in the current Colombia lake.

## Materialization contract

Each detector should produce a `signal_feature_<id>` curated table that can be consumed by the existing signal materializer. Minimum columns:

| column | expected value |
|---|---|
| `signal_id` | stable detector ID |
| `entity_id` | canonical entity UID or fallback `doc:<key>` / `project:<bpin>` |
| `entity_key` | exact document, NIT, BPIN, or item/order key |
| `entity_label` | `Company`, `Person`, `Project`, `Buyer`, or `ProcurementItem` |
| `scope_key` | stable dedup scope such as contract, buyer-supplier-role, project, order, or legal-period key |
| `scope_type` | detector scope: `contract`, `buyer`, `project`, `procurement_item`, `disclosure`, `legal_period` |
| `severity` | `medium`, `high`, or `critical`; default high for P0 reviewer queues |
| `risk_signal` | 0.0 to 1.0 score based on explicit additive factors |
| `identity_confidence` | 1.0 for exact document/NIT/BPIN; lower values should remain reviewer-only |
| `identity_match_type` | e.g. `EXACT_PERSON_DOCUMENT`, `EXACT_COMPANY_NIT`, `EXACT_BPIN` |
| `identity_quality` | usually `exact`; use `normalized_text` only for reviewer-only item comparisons |
| `evidence_refs` | list of source-prefixed refs, capped and ranked by value/date/relevance |

Default public policy: all detectors below start `reviewer_only: true` unless explicitly marked public-safe. A detector can become public only after legal-status parsing, identity policy, and `what_is_unproven` text are implemented.

## Priority ladder

| priority | detector | current readiness | first implementation form |
|---|---|---|---|
| P0 | `procurement_role_supplier_same_buyer_review_only` | materialized locally: 11,311 reviewer-only feature rows; downstream run produced 68,270 evidence rows | curated DuckDB view over SECOP II contract roles and award facts |
| P0 | `public_declaration_supplier_chronology_review_only` | materialized locally: 25,000 capped reviewer-only feature rows; downstream run produced 135,488 evidence rows | curated DuckDB view over declarations, sensitive positions, and SECOP natural-person supplier awards |
| P0/P1 | `public_declaration_company_bridge_current_risk_review_only` | materialized locally: 56 reviewer-only feature rows; downstream run produced 1,354 evidence rows; RUES representative bridge is not proof of control or conflict | DuckDB view over declaration/SIGEP person profiles, RUES legal representatives, and current compound-risk companies |
| P0 | `tvec_item_price_dispersion_review_only` | materialized locally: 4,601 reviewer-only feature rows; downstream run produced 13,877 evidence rows | curated DuckDB view over TVEC item/unit/quantity/year comparator rows |
| P0/P1 | `cuentas_claras_donor_ineligibility_review` | materialized locally: 322 reviewer-only feature rows; downstream run produced 1,220 evidence rows; legal cap/result inputs missing | curated DuckDB view with threshold-missing and election-result-missing state |
| P0 | `procurement_secop_sanction_later_awards_review_only` | materialized locally: 127 reviewer-only feature rows; downstream run produced 588 evidence rows; sanction finality/legal ineligibility unproven | curated DuckDB view over SECOP II sanctions and later exact supplier awards |
| P0 | `fiscal_procurement_chronology_review_only` | materialized locally: 80 reviewer-only feature rows; downstream run produced 400 evidence rows; legal finality/current disability/finding finality unproven | DuckDB view over Contraloria fiscal findings/responsibility plus later exact SECOP supplier exposure |
| P0 | `siri_antecedent_procurement_chronology_review_only` | materialized locally: 1,120 reviewer-only feature rows; downstream run produced 2,357 evidence rows; legal finality/ineligibility still reviewer-only | curated DuckDB view over SIRI antecedents plus exact supplier, contract-role, and RUES legal-representative procurement exposure |
| P0/P1 | `procurement_guarantee_advance_execution_chain` | materialized locally: 257 reviewer-only feature rows; v2 downstream run produced 1,505 evidence rows including 704 guarantee refs; invoice/payment-plan sources now loaded and split into adjacent detectors | DuckDB view over contract advance/pending-execution plus guarantee status/date/value, execution, suspension, modification, and SECOP-sanction chain |
| P0/P1 | `procurement_guarantee_policy_reuse_review_only` | materialized locally: 723 reviewer-only feature rows; selected run produced 3,110 evidence rows; policy validity and legitimate master-policy explanations remain reviewer-only | DuckDB view over SECOP guarantee insurer/policy clusters joined to exact SECOP II awards |
| P0/P1 | `procurement_budget_chain_reconciliation_review_only` | materialized locally: 1,000 capped reviewer-only feature rows; downstream run produced 3,784 evidence rows; invoice extension now split into a separate detector | DuckDB view over exact contract CDP, commitment, rubro, and guarantee/execution-chain support |
| P0/P1 | `procurement_invoice_budget_reconciliation_review_only` | materialized locally: 376 reviewer-only feature rows; downstream run produced 1,603 evidence rows; payment-plan source now loaded and split into an adjacent detector | DuckDB view over exact contract SECOP invoices plus budget-chain and guarantee/execution-chain context |
| P0/P1 | `procurement_payment_plan_reconciliation_review_only` | materialized locally: 1,000 reviewer-only feature rows; downstream run produced 3,934 evidence rows; SIIF/treasury/CUFE validation unproven | DuckDB view over exact contract SECOP payment plans plus invoice/budget/guarantee-chain context |
| P1 | `procurement_contract_modification_ladder_review_only` | materialized locally: 320 reviewer-only feature rows; downstream run produced 2,767 evidence rows; source modification values and legal effect require contract-file review | DuckDB view over exact contract SECOP modification sequences plus suspension/execution and already-materialized support-feature context |
| P1 | `procurement_related_bidders_same_process_review_only` | materialized locally: 16 reviewer-only feature rows; downstream run produced 226 evidence rows; shared representation is not proof of collusion or control | DuckDB view over exact SECOP process offers, award winners, and RUES shared-representative clusters |
| P1 | `procurement_shared_representative_same_buyer_cluster_review_only` | materialized locally: 166 reviewer-only feature rows; selected run produced 1,706 evidence rows; shared representation and same-buyer awards are not proof of collusion or control | DuckDB view over SECOP II awards joined to exact RUES legal representatives by supplier NIT, buyer, and year |
| P1 | `rues_supplier_capacity_status_review_only` | materialized locally: 1,044 reviewer-only feature rows; downstream run produced 6,499 evidence rows; branch/NIIF not loaded | DuckDB view over RUES status/renewal/registration-age/matricula flags plus SECOP exposure |
| P1 | `sgr_ocad_executor_capacity_gap` | materialized locally: 60 reviewer-only feature rows; downstream run produced 689 evidence rows; DNP context now split into adjacent detector | project-level DuckDB view using BPIN/SGR plus RUES/procurement-failure chains |
| P1 | `dnp_sgr_beneficiary_delivery_gap_review_only` | materialized locally: 60 reviewer-only feature rows; downstream run produced 1,887 evidence rows; DNP beneficiary context does not prove beneficiary harm or non-delivery | DuckDB view over exact-BPIN SGR capacity gap plus DNP executor/location/beneficiary sources |
| P1 | `cross_signal_compound_risk_review_only` | materialized locally: 1,000 reviewer-only feature rows; refreshed downstream run produced 18,791 evidence rows | downstream DuckDB view over materialized signal feature tables |
| P1 | `secop_i_legacy_supplier_current_risk_review_only` | materialized locally: 556 reviewer-only feature rows after guarantee-policy-reuse refresh; legacy/current continuity and irregularity remain unproven | DuckDB view over SECOP I exact-NIT legacy supplier rollups joined to current compound-risk entities |
| P1 | `secop_i_legacy_representative_current_risk_review_only` | materialized locally: 13 selected-run rows and 14 refreshed current rows; selected run produced 338 evidence rows; ownership/control and role-date continuity remain unproven | DuckDB view over SECOP I legal-representative rollups joined through current RUES legal representation to current compound-risk companies |
| P1 | `secop_interadmin_executor_network_review_only` | materialized locally: 702 reviewer-only feature rows after guarantee-policy-reuse refresh | DuckDB view over SECOP interadministrative agreements plus exact-NIT signal-family evidence |
| P1 | `health_pae_service_delivery_gap_review_only` | materialized locally: 639 reviewer-only feature rows; downstream run produced 2,470 evidence rows; service-line/beneficiary audit unproven | exact REPS provider or SECOP health/PAE object-text queue plus independent payment/budget/execution/competition support |
| P1 | `pae_beneficiary_territory_delivery_gap_review_only` | materialized locally: 64 reviewer-only feature rows; downstream run produced 607 evidence rows; MEN municipal beneficiary context is not proof of delivery failure | PAE-keyword service-delivery risk contracts plus MEN municipal beneficiary/population context |

## 1. `procurement_role_supplier_same_buyer_review_only`

Question: did a natural-person contract role holder also contract as a supplier to the same buyer?

Required sources:

- Loaded: `secop_ii_contracts`, curated `fct_procurement_contract_awards`.
- Useful joins: `conflict_disclosures`, `asset_disclosures`, `procurement_payment_plan_anomalies`, `procurement_contract_suspensions`, `procurement_large_modifications`.

Core keys:

- Role document: `supervisor_doc_number`, `spending_orderer_doc_number`, `payment_orderer_doc_number`.
- Supplier document: `supplier_document_digits` from awards.
- Buyer key: `buyer_document_digits` or `buyer_document_id`.

Hard gates:

- Role document must look like a person document, not a buyer NIT: 6 to 10 digits, not all zeros, not equal to buyer NIT, and not in `dim_buyer`.
- Require same buyer.
- Require at least one role contract and one supplier contract with distinct contract IDs.
- Prefer supplier award during or after role exposure; otherwise downgrade.

Score factors:

- +0.20 same buyer.
- +0.15 role date before or overlapping supplier award.
- +0.15 supplier value above COP 500M.
- +0.10 supplier contract count >= 3.
- +0.10 role field is spending/payment orderer.
- +0.10 also appears in declarations or SIGEP.
- +0.10 supplier contracts also have modification/suspension/payment anomalies.

Output:

- `entity_label`: `Person`.
- `entity_key`: role/supplier document digits.
- `scope_key`: `role_supplier:<buyer_doc>:<role_doc>:<role_field>`.
- `scope_type`: `buyer_role_supplier`.
- Evidence: top role contracts and supplier contracts, plus supporting anomaly feature refs.

False-positive controls:

- Do not emit public hits.
- Exclude institutional documents aggressively.
- Emit `what_is_unproven`: same document overlap does not prove conflict, employment status, or corrupt intent.

Case anchor: Centros Poblados shows why supervisor/interventor and validation failures are high-value review surfaces: https://www.procuraduria.gov.co/Pages/procuraduria-sanciono-exfuncionarios-mintic-y-union-temporal-centros-poblados-2020.aspx

## 2. `public_declaration_supplier_chronology_review_only`

Question: did a public declarant, conflict declarant, or sensitive-position holder later appear as a supplier or supplier officer in procurement?

Required sources:

- Loaded and implemented in the first pass: `8tz7-h3eu` asset disclosures, `gbry-rnq4` conflict disclosures, `5u9e-g5w9` sensitive positions, `secop_ii_contracts`.
- Loaded but not yet bridged in this detector: `company_registry_c82u` for legal-representative/company supplier exposure.

Core keys:

- Declarant document: `numero_documento`.
- Supplier document: exact person supplier document.
- Company bridge: RUES legal representative/officer document to supplier NIT.

Hard gates:

- Exact person document.
- At least one procurement exposure after or during declaration/position window.
- Rank same-buyer/same-entity exposure above national-level generic exposure.
- Require at least one declared interest signal, sensitive position, same-buyer overlap, or company bridge for high severity.

Score factors:

- +0.20 same entity/buyer.
- +0.15 sensitive position overlap.
- +0.15 declaration says contractor or private economic activity.
- +0.15 omitted/contradictory conflict fields relative to supplier exposure.
- +0.15 company officer bridge to supplier NIT.
- +0.10 donor overlap.
- +0.10 contract anomaly overlap.

Output:

- `entity_label`: `Person`.
- `scope_key`: `declaration_supplier:<doc>:<buyer_or_company>`.
- Evidence: declaration row, conflict row if present, sensitive-position row if present, top procurement awards, RUES bridge rows.

False-positive controls:

- Declarants can legally contract in some contexts; this is a chronology queue, not a finding.
- Public output should aggregate only by office/entity/territory until reviewer policy is explicit.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-declaration-supplier-20260605`, with 25,000 capped hits and 135,488 evidence rows. Aggregate validation: 24,589 distinct person documents, 209 buyers, and COP 2.64T pair-level contract value.

Case anchor: Vichada donor/contractor sanction highlights the value of chronology across public office, campaign finance, and procurement: https://www.procuraduria.gov.co/Pages/procuraduria-confirmo-destitucion-inhabilidad-ocho-anos-gobernador-Vichada-irregularidades-contrato.aspx

## 2A. `public_declaration_company_bridge_current_risk_review_only`

Question: did a public declarant, conflict declarant, or sensitive-position person connect through RUES legal-representative identity to a company already in current compound-risk evidence?

Required sources:

- Loaded and implemented: `asset_disclosures`, `conflict_disclosures`, `sigep_sensitive_positions`, `company_registry_c82u`, and `cross_signal_compound_risk_review_only` with its current source families.
- Useful extensions: beneficial ownership, RUES role-date history, official case bulletins, SIRI/fiscal joins by representative document.

Core keys:

- Person: exact declarant or SIGEP document.
- Company bridge: RUES exact legal-representative document to exact company NIT.
- Current support: exact-NIT compound-risk company row and ranked current evidence refs.

Hard gates:

- Exact person document and exact company NIT only.
- Company must already be in `cross_signal_compound_risk_review_only` with at least three signal families.
- Require conflict disclosure, contractor declaration, asset private/corporate/board-interest flag, or sensitive-position context with at least four current signal families.
- Reviewer-only because the bridge does not prove ownership, beneficial control, undeclared interest, conflict of interest, legal disability, contract irregularity, nonperformance, or corrupt intent.

Score factors:

- +0.04 current company has at least four signal families.
- +0.03 conflict disclosure.
- +0.03 explicit conflict flag.
- +0.03 asset corporate or board interest.
- +0.02 sensitive-position context.

Output:

- `entity_label`: `Company`.
- `entity_key`: exact company NIT.
- `scope_key`: `declaration_company_bridge:<person_doc>:<company_nit>`.
- `scope_type`: `declaration_company_bridge_current_risk`.
- Evidence: current compound-risk feature ref, current support refs, declaration/SIGEP refs, and RUES company registry ref.
- `what_is_unproven`: ownership, beneficial control, undeclared interest, conflict of interest, legal disability, contract irregularity, nonperformance, and corrupt intent.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-declaration-company-bridge-current-risk-20260605`, with 56 hits and 1,354 evidence rows. After guarantee-policy-reuse support refresh, the current table/full run emits 58 hits. Original aggregate validation: 34 critical rows, 22 high rows, 56 companies, 55 representative documents, 47 asset-disclosure rows, 47 conflict-disclosure rows, 22 SIGEP rows, 35 contractor-declaration rows, 17 asset-interest rows, and 9 explicit conflict-flag rows.

## 3. `tvec_item_price_dispersion_review_only`

Question: did a buyer pay unusually high unit prices for comparable TVEC items?

Required sources:

- Loaded: `3hdv-smhz` TVEC item purchases.
- Useful joins: `signal_feature_tvec_multi_entity_capture`, SECOP repeat awards, sanctions, suspensions, large modifications.

Core keys:

- Item comparator: exact catalog code if available; otherwise normalized `item_name`, `unit`, quantity band, category/date window.
- Supplier: `supplier_nit`.
- Buyer: `buyer_nit`.
- Order: `order_id`.

Hard gates:

- Unit price and line total must be positive and parseable.
- Comparable group must have at least 20 rows, 3 suppliers, and 5 buyers.
- Use p90/median ratio or robust z-score; initial threshold p90 >= 2x median.
- Exclude generic bundle labels such as `presupuesto` unless a stronger catalog code exists.
- Start with reviewer-only item/order hits.

Score factors:

- +0.25 unit price >= p95 within comparable group.
- +0.20 line total above COP 100M.
- +0.15 supplier is also in TVEC multi-entity capture.
- +0.10 same buyer has repeated high-price orders.
- +0.10 supplier has SECOP concentration/repeat-award signals.
- +0.10 emergency/health/PAE/defense keyword category.
- +0.10 scarce competition or direct modality when linked to SECOP.

Output:

- `entity_label`: `ProcurementItem` or `Company` depending on UI need.
- `entity_key`: `<supplier_nit>` for supplier queue; include item key in scope.
- `scope_key`: `tvec_price:<item_key>:<order_id>:<supplier_nit>`.
- Evidence: TVEC order line, comparable group stats, top comparator examples, linked supplier signals.

Local sizing from rough profile:

- 1,001,457 valid price rows.
- 2,423 comparable item/unit groups.
- 861 high-dispersion groups.
- 12,188 high-price lines across 7,941 orders, 581 suppliers, about COP 3.53T line value.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-tvec-price-20260605`, with 4,601 hits and 13,877 evidence rows. Aggregate validation: 432 normalized item keys, 3,297 orders, 412 suppliers, 413 buyers, and COP 1.74T flagged line value. Current comparator gates are item name, unit, quantity band, year, at least 20 lines, 3 suppliers, 5 buyers, p90 >= 2x median, and observed unit price >= p95 and >= 2x median.

Case anchors:

- Encino biosecurity inputs overpricing: https://www.procuraduria.gov.co/Pages/presunto-sobrecosto-compra-insumos-bioseguridad-procuraduria-formulo-cargos-exalcalde-encino-santander.aspx
- Suba first-aid inputs overpricing: https://www.procuraduria.gov.co/Pages/presuntos-sobrecostos-insumos-medicos-procuraduria-indaga-funcionarios-alcaldia-suba.aspx

## 4. `cuentas_claras_donor_ineligibility_review`

Question: did a donor later contract with entities controlled by the elected campaign's office/jurisdiction during the legally relevant period?

Required sources:

- Loaded: `cuentas_claras_income_2019`, `secop_ii_contracts`.
- Missing/needed for legal classification: election results and campaign spending caps or configured threshold table.

Core keys:

- Donor document/NIT: `ing_identificacion` or equivalent.
- Candidate: candidate ID/name/office/party/jurisdiction.
- Supplier: `supplier_document_digits` / canonical NIT.
- Buyer: SECOP buyer NIT and territory.

Hard gates:

- Exact donor-to-supplier document/NIT.
- Contract signing date during inferred 2020-2023 local term.
- Same jurisdiction/administrative level: mayor by department+municipality, governor by department.
- Donation amount above configured review floor; emit `missing_campaign_cap` until legal cap data is loaded.

Score factors:

- +0.25 donation exceeds legal threshold once campaign caps are loaded.
- +0.20 same jurisdiction and office level.
- +0.15 direct contracting or low competition.
- +0.15 repeated awards or high aggregate supplier value.
- +0.10 donor is declarant/public servant/company officer.
- +0.10 contract anomaly overlap.

Output:

- `entity_label`: `Company` for supplier NIT or `Person` for natural-person supplier.
- `scope_key`: `donor_contract:2019:<office_level>:<campaign_key>:<donor_doc>:<buyer_doc>`.
- Evidence: donor voucher, candidate/campaign row, award refs, threshold evidence if available.

False-positive controls:

- Do not classify legal ineligibility unless threshold, elected office, term, and jurisdiction are known.
- When any legal element is missing, keep severity <= medium and label as `missing_campaign_cap`, `missing_election_result`, and `needs_legal_threshold_and_election_result`.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-cuentas-ineligibility-20260605`, with 322 hits and 1,220 evidence rows. Aggregate validation: 190 donor/supplier documents, 120 campaigns, 142 buyers, COP 2.919T term contract value, and COP 3.984T campaign income. Split: 164 mayor/municipality rows and 158 governor/department rows. This is a review queue, not a legal ineligibility classifier.

Case anchor: Vichada governor/contributor sanction: https://www.procuraduria.gov.co/Pages/procuraduria-confirmo-destitucion-inhabilidad-ocho-anos-gobernador-Vichada-irregularidades-contrato.aspx

## 5. `procurement_secop_sanction_later_awards_review_only`

Question: did a supplier with a SECOP II sanction, multa, clausula penal, or incumplimiento record later receive additional awards?

Required sources:

- Loaded: `secop_sanctions` (`it5q-hg94`), `secop_ii_contracts`.
- Useful joins: PACO sanctions, same-buyer role/supplier overlap, public declarations, Cuentas donor queue, suspensions, modifications, payment anomalies.

Core keys:

- Sanction contract: `id_contrato` in SECOP sanctions.
- Supplier: exact supplier document resolved from the sanctioned SECOP II contract.
- Later awards: same exact supplier document, later signing date, different contract ID.

Hard gates:

- Exact sanction contract ID to SECOP II contract match.
- Exact supplier document from sanctioned contract to later awards.
- Exclude draft sanction records.
- Require at least one later award and either COP 100M later value or same-buyer later exposure.

Score factors:

- +0.15 same buyer after sanction.
- +0.08 platform status is published by administrator.
- +0.12 later-award aggregate value.
- +0.08 later contract count.
- +0.07 direct/exception later modality.
- +0.07 sanction amount.

Output:

- `entity_label`: `Company` or `Person`.
- `scope_key`: `secop_sanction_later_awards:<sanction_contract>:<supplier_doc>:<sanction_row>`.
- Evidence: SECOP sanction row, sanctioned contract, top later award refs.

False-positive controls:

- Reviewer-only.
- Emit `what_is_unproven`: final sanction status, appeal status, legal ineligibility, and corrupt intent are not proven by this feature.
- Treat `A la espera de aprobación` as review context, not final legal status.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-secop-sanction-later-awards-20260605`, with 127 hits and 588 evidence rows. Aggregate validation: 103 exact suppliers, 120 sanctioned contracts, 47 sanctioning buyers, 1,928 later contract references, COP 817.28B later-award value, and COP 402.81B same-buyer later-award value.

Case anchor: Centros Poblados shows why sanction/guarantee/execution chronology matters around supplier conduct and validation failures: https://www.procuraduria.gov.co/Pages/procuraduria-sanciono-exfuncionarios-mintic-y-union-temporal-centros-poblados-2020.aspx

## Fiscal extension: `fiscal_procurement_chronology_review_only`

Question: did a company/person with Contraloria fiscal finding or fiscal-responsibility records later appear as a SECOP supplier?

Required sources:

- Loaded and implemented: `8qxx-ubmq` Hallazgos Fiscales, `jr8e-e8tu` Responsabilidad Fiscal, and SECOP II contracts.
- Useful joins: SECOP/PACO sanctions, SIRI antecedents, RUES capacity/status, interadministrative recipient networks, and cross-signal compound risk.

Core keys:

- Fiscal subject document: `nit`, `identificaci_n`, or `n_mero_de_identificaci_n`, normalized to exact document key.
- Fiscal record: finding radicado or fiscal-responsibility resolution, with row-number suffix for stable dedup.
- Procurement exposure: exact supplier document, later contract signing date, later contract count/value.

Hard gates:

- Exact document/NIT match only.
- Require fiscal event/finality/procedure date and at least one later SECOP supplier award.
- Aggregate one queue row per fiscal source row and exact subject document.
- Rank fiscal-responsibility rows higher than finding rows; do not treat findings as final liability.

Score factors:

- +0.25 fiscal-responsibility record.
- +0.20 later-award value and count.
- +0.15 direct/exception later modality.
- +0.10 public-entity subject flag as reviewer context only.
- +0.10 same identity also appears in sanction/SIRI/cross-signal support.

Output:

- `entity_label`: `Company` or `Person`.
- `scope_key`: `fiscal_procurement:<source>:<record_id>:<document_key>`.
- Evidence: fiscal source row plus top later SECOP contract refs.
- `what_is_unproven`: current legal disability, final fiscal liability for finding rows, contract illegality, public-entity role misuse, and corrupt intent.

False-positive controls:

- Reviewer-only.
- Do not classify public-entity subject rows as corrupt supplier conduct without source-document review.
- Keep legal finality/current-disability and appeal status outside the detector until source fields are parsed and validated.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-fiscal-procurement-chronology-v2-20260605`, with 80 hits and 400 evidence rows. Aggregate validation: 32 entities, 80 fiscal records, 5,699 later contract references, COP 26.154T later procurement value, and COP 342.17B fiscal amount. Severity split: 47 critical, 27 high, and 6 medium. Source split: 47 fiscal-responsibility critical rows with COP 25.663T later value and COP 335.37B fiscal amount; 27 fiscal-finding high rows with COP 489.48B later value and COP 6.756B fiscal amount; 6 fiscal-finding medium rows with COP 2.272B later value and COP 39.1M fiscal amount.

## 6. `siri_antecedent_procurement_chronology_review_only`

Question: did a person/company with official antecedent or ineligibility records participate in procurement during a relevant period?

Required sources:

- Loaded and implemented in the first pass: `iaeu-rcn6` SIRI antecedents, SECOP II contracts, role documents, RUES legal-representative bridge.
- Useful extension joins: SIGEP/declarations, Cuentas donor queue, PACO/SECOP sanctions, and fiscal chronology cross-support.

Core keys:

- SIRI document: `numero_identificacion` mapped to `document_id`.
- SIRI process: `numero_proceso`; SIRI record: `numero_siri`.
- Procurement documents: supplier, RUES legal representative, supervisor, spending orderer, payment orderer.

Hard gates:

- Parse legal-effects date (`dd/mm/yyyy`) and duration fields.
- Separate "antecedent exists" from active-ineligibility inference; emit explicit status buckets.
- Exact document matching only.
- Require procurement exposure on or after the SIRI legal-effects date.
- Keep duration-missing post-effect rows only for severe sanction text and role exposure or contract value >= COP 100M.
- Person hits reviewer-only.

Score factors:

- +0.30 active-ineligibility-inferred period overlaps contract date.
- +0.20 same person appears as orderer/supervisor or supplier/representative.
- +0.15 role exposure in SECOP contract fields.
- +0.15 high-value contract exposure.
- +0.10 severe sanction text such as inhabilidad, destitucion, suspension, separacion, or exclusion.
- +0.10 donor/declaration/sanction/fiscal overlap when extension joins are added.

Output:

- `entity_label`: `Person` or `Company`.
- `scope_key`: `siri_procurement:<doc>:<exposure_type>:<contract_id>:<siri_record_id>`.
- Evidence: SIRI row, procurement role/award row, RUES company row for legal-representative bridge, sanction/fiscal rows if extension joins are added.

False-positive controls:

- Do not expose as public until active legal status can be proven from SIRI fields.
- Emit status bucket: `active_ineligibility_inferred` or `duration_missing_post_effect`.
- Emit `what_is_unproven`: the feature does not prove finality, appeal status, current legal disability, exceptions, identity beyond exact document equality, or corrupt intent.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-siri-antecedent-20260605`, with 1,120 hits and 2,357 evidence rows. Aggregate validation: 103 exact person documents, 768 contracts, COP 47.59B procurement value, 903 critical active-ineligibility-inferred rows, 162 high active-ineligibility-inferred rows, and 55 medium duration-missing-post-effect rows. Exposure split: 475 supervisor rows, 362 spending-orderer rows, 166 direct-supplier rows, and 117 RUES legal-representative supplier rows.

## 7. `procurement_guarantee_advance_execution_chain`

Question: did a contract with advance payment have weak/missing/late guarantees and then show poor execution, suspensions, modifications, sanctions, or payment anomalies?

Required sources:

- Loaded and implemented: `secop_ii_contracts`, `gjp9-cutm` SECOP II guarantees, `mfmm-jqmq`, `u99c-7mfm`, `u8cx-r425`, `it5q-hg94`.
- Loaded adjacent extensions: `ibyt-yi2f` invoices are handled by `procurement_invoice_budget_reconciliation_review_only`, and `uymx-8p3j` payment-plan timing is handled by `procurement_payment_plan_reconciliation_review_only`.

Core keys:

- Contract: `id_contrato`.
- Supplier: canonical NIT.
- Guarantee: policy number/type/subtype/status, creation/send/end dates, and value.

Hard gates:

- Contract value at least COP 500M and exact supplier document.
- High advance payment or ended contract with material pending-execution value.
- At least one independent execution red flag: execution delay, suspension, large modification, or SECOP sanction.
- Require at least three execution-chain flags.
- Guarantee missing/no-accepted/rejected/draft/cancelled/late-sent/value-gap flags escalate reviewer priority but do not prove guarantee invalidity.

Score factors:

- +0.25 advance value >= COP 1B or >= 20 percent of contract value.
- +0.20 guarantee missing/late/invalid.
- +0.15 paid or invoiced value high while execution is low.
- +0.15 suspension/modification after advance.
- +0.10 sanction/fiscal hit.
- +0.10 same role-to-supplier overlap.
- +0.10 priority BPIN/SGR/OCAD project.

Output:

- `entity_label`: `Company` or `Person`.
- `scope_key`: contract ID.
- `scope_type`: `contract_execution_chain`.
- Evidence: contract, guarantee rows, execution row, suspension/modification rows, and SECOP sanctions.
- Emit `guarantee_source_status` buckets such as `no_secop_guarantee_row`, `guarantee_rows_without_accepted_pending`, `guarantee_rows_without_accepted_rejected`, `accepted_guarantee_present_but_sent_after_start`, and `accepted_or_expired_guarantee_present`.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-guarantee-advance-chain-v2-20260605`, with 257 hits and 1,505 evidence rows. Aggregate validation: 243 exact suppliers, 101 buyers, COP 1.203T contract value, COP 251.30B advance-payment value, and COP 1.066T pending-execution value. Severity split remains 21 critical and 236 high. Guarantee split: 235 rows with guarantee records, 194 with accepted-policy evidence, 22 with no SECOP guarantee row, 41 with guarantee rows without accepted policy, 25 with first policy sent after contract start, and 139 with at least one guarantee issue flag.

Case anchors:

- Centros Poblados guarantee/advance-payment validation failures: https://www.procuraduria.gov.co/Pages/procuraduria-sanciono-exfuncionarios-mintic-y-union-temporal-centros-poblados-2020.aspx
- UNGRD delivery and policy concerns: https://www.procuraduria.gov.co/Pages/procuraduria-abrio-nueva-investigacion-olmedo-lopez-exdirector-ungrd-posibles-irregularidades-compra-tanques.aspx

## 7A. `procurement_guarantee_policy_reuse_review_only`

Question: does the same SECOP guarantee insurer and policy number appear on accepted or expired guarantee records for different suppliers and different buyers?

Required sources:

- Loaded and implemented: `gjp9-cutm` SECOP II guarantees and `secop_ii_contracts`.

Core keys:

- Guarantee cluster: normalized `insurer` + normalized `policy_number`.
- Contract: exact SECOP II `contract_id`.
- Supplier/buyer: exact awarded supplier document key and buyer document digits from `curated_contract_awards`.

Hard gates:

- Exact contract join from guarantee row to awarded contract.
- Policy-like number: normalized policy key length >= 8, at least 8 digits, non-placeholder insurer key, and no all-zero/all-one/all-nine policy keys.
- Exclude text placeholder policy values such as `No Definido`, `cumplimiento`, `poliza`, `seguro`, `garantia`, `responsabilidad`, `anexo`, insurer names, and broad contract/guarantee descriptions.
- Cluster must have 2 to 5 distinct contracts, at least 2 distinct suppliers, at least 2 distinct buyers, every member accepted or expired, and at least COP 1B aggregate contract value.

Score factors:

- +0.08 to +0.12 for small cluster size.
- +0.05 exact different suppliers.
- +0.05 exact different buyers.
- +0.04 all accepted/expired SECOP guarantee rows.
- +0.04 very high cluster value: at least COP 50B aggregate or COP 20B max contract.

Output:

- `entity_label`: `Company` or `Person`, based on awarded supplier identity.
- `scope_key`: `guarantee_policy_reuse:<insurer_key>:<policy_key>:<contract_id>`.
- `scope_type`: `guarantee_policy_cluster`.
- Evidence: current contract URL/ref, current SECOP guarantee row ref, and other cluster member contract/guarantee refs.
- Emit cluster metrics: contract/supplier/buyer counts, accepted/expired counts, total/max contract value, first/last signing date, department/sector spread, and policy-like/different-supplier/different-buyer/small-cluster flags.
- `what_is_unproven`: false policy, invalid policy, insurer denial, authorization, legal breach, and corrupt intent.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-guarantee-policy-reuse-20260605`, with 723 hits and 3,110 evidence rows. Aggregate validation: 345 insurer-policy clusters, 660 suppliers, 317 buyers, 701 distinct contracts, COP 2.248T row contract value, 62 critical rows, and 661 high rows. Cluster-size split: 317 two-contract clusters, 23 three-contract clusters, and 5 four-contract clusters. After downstream refresh, 43 cross-signal rows, 64 interadministrative rows, 11 SECOP I legacy/current rows, and 3 declaration-company bridge rows carry guarantee-policy-reuse support.

Case anchor: Centros Poblados false-guarantee and advance-payment validation failures motivate guarantee-identity review, but this detector does not prove a reused policy is false without insurer and contract-file validation: https://www.procuraduria.gov.co/Pages/procuraduria-sanciono-exfuncionarios-mintic-y-union-temporal-centros-poblados-2020.aspx

## 8. `procurement_budget_chain_reconciliation_review_only`

Question: did a high-value contract have missing, weak, or materially mismatched CDP, commitment, or rubro support, especially when guarantee/advance/execution-chain evidence is also present?

Required sources:

- Loaded and implemented: `secop_ii_contracts`, `a86w-fh92` CDP requests, `skc9-met7` commitments, `cwhv-7fnp` budget items/rubros, guarantees, execution, suspensions, modifications, and SECOP sanctions through the guarantee-chain context.
- Loaded adjacent extensions: `ibyt-yi2f` invoices are handled by `procurement_invoice_budget_reconciliation_review_only`, and `uymx-8p3j` payment plans are handled by `procurement_payment_plan_reconciliation_review_only`.

Core keys:

- Contract: exact SECOP II `id_contrato`.
- Supplier: canonical NIT or person document from the contract award.
- Budget support: CDP code/status/value, commitment ID/item/status/value, rubro ID/code/name/value.

Hard gates:

- Contract value at least COP 500M and exact supplier document.
- Emit when a guarantee/advance/execution-chain row has any budget issue, or when high-value contracts have multiple budget issues.
- Budget issues include missing CDP, missing commitment, missing rubro, only weak SIIF CDP status, only failed commitment integration, material CDP/commitment under-support, commitment over-support, or zero/undefined rubro.
- Reviewer-only until SIIF/source-document review validates the legal budget effect.

Output:

- `entity_label`: `Company` or `Person`.
- `scope_key`: contract ID.
- `scope_type`: `contract_budget_chain`.
- Evidence: SECOP contract URL/ref, up to three CDP refs, up to three commitment refs, up to three rubro refs, and guarantee-chain feature ref when present.
- Emit `what_is_unproven`: the flags do not prove illegal payment, budget availability, accounting breach, or corrupt intent without SIIF/source-document review.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-budget-chain-20260605`, with 1,000 capped hits and 3,784 evidence rows. Aggregate validation: 827 exact suppliers, 368 buyers, COP 1,364.83T capped queue value, 982 critical rows, 18 high rows, and 257 rows with guarantee-chain evidence. Budget flags: 923 weak-CDP-status rows, 287 failed-commitment rows, 687 missing commitments, 687 missing rubros, 801 CDP-under-contract rows, 826 commitment-under-contract rows, 141 commitment-over-contract rows, and 310 zero/undefined-rubro rows.

## 9. `procurement_invoice_budget_reconciliation_review_only`

Question: did a high-value contract have confirmed or countable SECOP invoice rows that exceed the contract value, occur after contract end, or contradict the contract's paid/invoiced summary, especially when budget-chain or guarantee/execution-chain context is also present?

Required sources:

- Loaded and implemented: `secop_ii_contracts`, `ibyt-yi2f` SECOP II invoices, `procurement_budget_chain_reconciliation_review_only`, `procurement_guarantee_advance_execution_chain`.
- Loaded adjacent extension: `uymx-8p3j` payment plans are handled by `procurement_payment_plan_reconciliation_review_only` for real payment dates, CUFE, supervisor document, and commitment references.
- Supporting context: CDP requests, commitments, rubros, guarantees, execution, suspensions, modifications, and SECOP sanctions through the upstream reviewer queues.

Core keys:

- Contract: exact SECOP II `id_contrato`.
- Invoice: `id_pago`, `numero_de_factura`, status, confirmation flag, invoice date, delivery date, estimated-payment date, `valor_total`, `valor_a_pagar`, and `valor_neto`.
- Supplier: canonical NIT or person document from the contract award.

Hard gates:

- Exact contract ID join and one de-duplicated contract-award row per contract.
- Contract value at least COP 100M and exact supplier document.
- Use decimal-safe parsing for invoice monetary strings; do not use digit-stripping parsing for SECOP invoice decimal fields.
- Exclude rejected, cancelled, annulled, draft, or deleted invoice statuses from value rollups.
- Emit when invoice value exceeds contract value by at least 25 percent and COP 100M with budget/guarantee context or large excess; when material post-end invoice value has budget/guarantee context; or when invoice rows contradict a zero paid/invoiced contract summary with both budget and guarantee context.

Score factors:

- +0.055 per invoice issue flag, capped at +0.24.
- +0.06 budget-chain context.
- +0.06 guarantee/advance/execution-chain context.
- +0.05 confirmed invoice value exceeds contract value.
- +0.04 post-contract-end invoice value.
- +0.10 contract value scale cap.

Output:

- `entity_label`: `Company` or `Person`.
- `scope_key`: exact SECOP contract ID.
- `scope_type`: `contract_invoice_budget_reconciliation`.
- Evidence: SECOP contract ref, top three invoice refs, budget-chain feature ref when present, and guarantee-chain feature ref when present.
- Emit `what_is_unproven`: invoice/budget reconciliation does not prove illegal payment, duplicate disbursement, accounting breach, delivery failure, or corrupt intent without SIIF, treasury, and contract-file review.

False-positive controls:

- Keep reviewer-only.
- Distinguish source invoice rows from actual treasury disbursements; even with `uymx-8p3j`, SIIF or treasury records are still needed before any payment-irregularity statement.
- Treat post-end invoices as review cues; contract liquidation, late billing, amendments, and corrected invoices can be legitimate.
- Sample high-value rows before public use because SECOP invoice sources can contain repeated invoice IDs, status transitions, and decimal monetary strings.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-invoice-budget-reconciliation-20260605`, with 376 hits and 1,603 evidence rows after decimal-safe parsing and contract-award de-duplication. Aggregate validation: 343 exact suppliers, 156 buyers, 149 critical rows, and 227 high rows. Flag split: 278 invoice-value-over-contract rows, 45 confirmed-invoice-over-contract rows, 159 post-contract-end invoice rows, 60 invoice-summary-zero-gap rows, and 44 estimated-payment-before-invoice rows. Context split: 99 budget-chain overlaps, 92 guarantee-chain overlaps, and 92 rows with both.

## 9A. `procurement_payment_plan_reconciliation_review_only`

Question: did a high-value contract have real-paid SECOP payment-plan rows exceeding contract value, post-end real payments, duplicate CUFE use, supplier-document mismatch, or payment-before-invoice chronology, especially when invoice, budget, or guarantee context is also present?

Required sources:

- Loaded and implemented: `secop_ii_contracts`, `uymx-8p3j` SECOP II payment plans, `procurement_invoice_budget_reconciliation_review_only`, `procurement_budget_chain_reconciliation_review_only`, and `procurement_guarantee_advance_execution_chain`.
- Supporting context: SECOP invoices, CDP requests, commitments, rubros, guarantees, execution, suspensions, modifications, and SECOP sanctions through the upstream reviewer queues.

Core keys:

- Contract: exact SECOP II `id_contrato`.
- Payment plan: `id_pago`, payment status, `fecha_real_de_pago`, `valor_actual_pagado`, CUFE, provider document, supervisor document, and `compromiso_presupuestal`.
- Invoice chronology: invoice issue/receipt dates and payment-plan real payment date.

Hard gates:

- Exact contract ID join and one de-duplicated contract-award row per contract.
- Contract value at least COP 100M and exact supplier document.
- Roll up only paid/approved-ish actual-payment rows; exclude rejected, cancelled, annulled, draft, or deleted statuses where status text allows.
- Emit when actual paid value exceeds contract value by at least 25 percent and COP 100M, when real payments occur after contract end with adjacent context, when CUFE repeats across paid rows on different contracts, when paid rows predate invoice issue/receipt dates, or when payment-provider document conflicts with the awarded supplier.
- Keep reviewer-only because SECOP payment-plan rows are not treasury/SIIF proof.

Score factors:

- +0.16 actual paid value exceeds contract value.
- +0.10 real payment after contract end.
- +0.10 payment-before-invoice chronology.
- +0.08 supplier-document mismatch.
- +0.12 duplicate CUFE across contracts.
- +0.06 invoice-budget context.
- +0.06 budget-chain context.
- +0.06 guarantee/advance/execution-chain context.
- +0.10 contract value scale cap.

Output:

- `entity_label`: `Company` or `Person`.
- `scope_key`: exact SECOP contract ID.
- `scope_type`: `contract_payment_plan_reconciliation`.
- Evidence: SECOP contract ref, top payment-plan refs by issue severity/value/date, invoice-budget feature ref when present, budget-chain feature ref when present, and guarantee-chain feature ref when present.
- Emit `what_is_unproven`: payment-plan reconciliation does not prove illegal payment, duplicate disbursement, accounting breach, delivery failure, or corrupt intent without SIIF, treasury, CUFE validation, and contract-file review.

False-positive controls:

- Keep reviewer-only.
- Treat post-end payments as review cues; liquidation, late billing, amendments, or corrected records can be legitimate.
- Duplicate CUFE may reflect correction, reissue, or source repetition rather than duplicate disbursement.
- Supplier-document mismatches can reflect consortium, cession, correction, or source-entry issues.
- Sample high-value rows before public use because payment-plan status semantics and CUFE reuse require source-file validation.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-payment-plan-reconciliation-20260605`, with 1,000 hits and 3,934 evidence rows. Aggregate validation: 311 exact suppliers, 210 buyers, COP 6.734T contract value, and 1,000 critical rows. Flag split: 993 actual-paid-over-contract rows, 650 real-payment-after-contract-end rows, 243 payment-before-invoice rows, 7 supplier-document-mismatch rows, and 24 duplicate-CUFE-across-contract rows. Context split: 28 invoice-budget overlaps, 2 budget-chain overlaps, and 1 guarantee-chain overlap. Evidence source split: 2,903 payment-plan refs, 1,000 SECOP contract refs, 28 invoice-budget feature refs, 2 budget-chain feature refs, and 1 guarantee-chain feature ref.

## 9B. `procurement_contract_modification_ladder_review_only`

Question: did a contract accumulate a sequence of SECOP modifications that changed value, time, payment terms, scope, or works, especially when independent execution/payment/budget/competition support exists?

Required sources:

- Loaded and implemented: `secop_ii_contracts`, `u8cx-r425` contract modifications, `u99c-7mfm` suspensions, and `mfmm-jqmq` execution rows.
- Adjacent support used when feature tables already exist or during full builds: large modifications, guarantee/advance chain, budget-chain, invoice-budget, payment-plan, single-bidder, and same-buyer role/supplier queues.

Core keys:

- Contract: exact SECOP II `id_contrato`.
- Supplier: canonical NIT or person document from the contract award.
- Modification: modification/requerimiento ID, status, purpose, description, value, extended days, and approval/version/load/create dates.

Hard gates:

- Exact contract ID join and one de-duplicated contract-award row per contract.
- Contract value at least COP 500M and exact supplier document.
- At least three SECOP modification rows.
- Require at least one core modification dimension: material value, major delay, payment/financial terms, or scope/change text.
- Emit when core dimensions and support converge, or when a high-count/material-value sequence is independently review-worthy.
- Keep reviewer-only because source modification value, versioning, legal effect, and competition implications require contract-file review.

Score factors:

- Modification event count, capped.
- Modification value share, capped.
- Total extended days, capped.
- Chain flag count across value, delay, financial terms, scope, modality, suspension, execution, guarantee, budget, invoice, payment, competition, and role support.
- Support signal count from adjacent exact-contract or same-buyer role queues.

Output:

- `entity_label`: `Company` or `Person`.
- `scope_key`: exact SECOP contract ID.
- `scope_type`: `contract_modification_ladder`.
- Evidence: SECOP contract ref, up to six ranked SECOP modification refs, and support-feature refs when present.
- Emit `what_is_unproven`: repeated modifications and support convergence do not prove illegal modification, unlawful avoidance of competition, fiscal harm, delivery failure, or corrupt intent without source-file and legal review.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-modification-ladder-20260605`, with 320 hits and 2,767 evidence rows. Aggregate validation: 307 suppliers, 129 buyers, 264 critical rows, 56 high rows, COP 1.489T queued contract value, COP 104.248T source modification value, and 130,377 total extended days. Support split: 309 large-modification overlaps, 132 single-bidder overlaps, 48 budget-chain overlaps, 47 guarantee-chain overlaps, 39 suspension overlaps, 14 invoice overlaps, and 1 execution-delay overlap. Refreshed hidden-connection queues now carry ladder support in 126 cross-signal rows, 161 interadministrative rows, 48 SECOP I legacy/current rows, and 5 declaration-company bridge rows.

Case anchor: Fiscalia's Odebrecht case page repeatedly identifies Ruta del Sol II contractual additions/modifications, payment-condition changes, and favoring of the concessionaire: https://www.fiscalia.gov.co/caso-odebrecht/

## 9C. `procurement_related_bidders_same_process_review_only`

Question: did the winning supplier and a competing offerer in the same SECOP II process share an exact RUES representative?

Required sources:

- Loaded: `secop_offers`, `secop_ii_contracts`, `company_registry_c82u`.
- Curated support: `curated_related_companies_shared_officer`.

Core keys:

- `process_key`: exact SECOP II process id.
- `winner_document_key`: exact winning supplier NIT from SECOP II contracts.
- `related_offerer_document_key`: exact competing offerer NIT from SECOP II offers.
- `representative_document_key`: exact RUES legal-representative/officer document.

Hard gates:

1. Exact process id match between award and offer rows.
2. Winner and offerer are distinct exact-NIT companies.
3. Both companies share the same exact RUES representative document.
4. Process has at least two valid offerers after placeholder exclusion.
5. Awarded contract value is at least COP 500M.
6. At least one reinforcing gate: process has <=8 valid offerers, the same pair repeats across material processes, or award value is at least COP 1B.

Score factors:

- Bounded competition and <=3-offerer processes.
- Repeated winner/counterpart/representative pair.
- COP 1B+ contract value.
- Shared-officer cluster size and contract value.

Output:

- `entity_label`: `Company`.
- `entity_key`: winning supplier exact NIT key.
- `scope_type`: `related_bidders_process`.
- `scope_key`: `related_bidders_process:<process_key>:<winner_key>:<related_offerer_key>:<representative_key>`.
- Evidence refs: SECOP contract URL/ref, SECOP offer ref, both shared-officer feature refs, and RUES/contract evidence carried by the shared-officer queue.

Reviewer caveat:

- Shared representation can be lawful corporate-group, insurance, consortium, or business-group structure. This detector does not prove collusion, beneficial ownership, control, simulated competition, bid suppression, legal ineligibility, breach, or corrupt intent.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-related-bidders-20260605`, with 16 hits and 226 evidence rows. Aggregate validation: 7 winning suppliers, 7 related offerers, 16 processes, 15 buyers, 13 critical rows, 3 high rows, and COP 18.93B queued contract value. Refreshed hidden-connection queues now carry related-bidders support in 3 cross-signal rows, 1 interadministrative row, and 2 SECOP I legacy/current rows.

## 9D. `procurement_shared_representative_same_buyer_cluster_review_only`

Question: did one public buyer award multiple distinct supplier companies tied
to the same exact RUES legal representative in the same year?

Required sources:

- Loaded: `secop_ii_contracts`, `company_registry_c82u`.
- Downstream support: `cross_signal_compound_risk_review_only`,
  `secop_i_legacy_supplier_current_risk_review_only`, and
  `secop_i_legacy_representative_current_risk_review_only` after refresh.

Core keys:

- `representative_document_key`: exact RUES legal-representative document.
- `buyer_document_id`: exact buyer NIT/document from SECOP II awards.
- `signing_year`: award signing year.
- `supplier_document_key`: exact supplier company NIT.

Hard gates:

1. Supplier must join SECOP II award to RUES by exact company NIT.
2. Representative document must be exact, non-placeholder, and different from
   the company document.
3. Cluster is representative + buyer + signing year.
4. Cluster has two to six distinct supplier companies and at least three
   contracts.
5. Cluster total contract value is at least COP 5B.
6. Representative is linked to at most ten current RUES companies.
7. At least one reinforcing gate: two direct/exception contracts, COP 10B+
   cluster exposure, or at least five contracts.

Score factors:

- Cluster total value.
- Supplier count and contract count.
- Direct/exception contracting support.
- COP 10B+ exposure.
- Representative linked to five or fewer companies.

Output:

- `entity_label`: `Company`.
- `entity_key`: supplier exact NIT key.
- `scope_type`: `shared_representative_same_buyer_cluster`.
- `scope_key`: `shared_representative_same_buyer:<representative_key>:<buyer_key>:<year>:<supplier_key>`.
- Evidence refs: current RUES registry refs, ranked supplier contract refs, and
  ranked cluster contract refs.
- Flags: exact representative bridge, same-buyer multi-supplier cluster,
  high-value cluster, direct/high-value/high-volume pressure, and bounded
  representative-company count.

Reviewer caveat:

- Shared legal representation and same-buyer awards can be lawful. This
  detector does not prove collusion, beneficial ownership, control, simulated
  competition, legal ineligibility, breach, nonperformance, or corrupt intent.

Implementation status: wired in ETL/registry and materialized locally in run
`phase-missing-patterns-shared-representative-same-buyer-20260606`, with 166
hits and 1,706 evidence rows. Aggregate validation: 77
representative/buyer/year clusters, 44 representatives, 35 buyers, 95
suppliers, 15 critical rows, 151 high rows, and COP 2.201T supplier exposure.
Refreshed hidden-connection queues now carry this support in 26 cross-signal
rows, 10 SECOP I legacy/current supplier rows, and 4 SECOP I
legal-representative/current-risk rows.

## 10. `rues_supplier_capacity_status_review_only`

Question: did a supplier receive contracts inconsistent with its legal/registration/capacity profile?

Required sources:

- Loaded and implemented in the first pass: `company_registry_c82u`, SECOP II contracts.
- Needed for stronger version: `nb3d-v3n7` branches, NIIF/Supersoc financial statements, RUES chambers.

Core keys:

- Supplier NIT.
- RUES status, matricula, registration date, renewal date, cancellation date.
- CIIU/activity versus SECOP object/category.

Hard gates:

- Exact NIT match.
- One or more status/capacity red flags: cancelled/inactive, stale renewal, very recent registration before large award, status-date contradiction, CIIU/object mismatch, or branch/domicile pattern.
- Reviewer-only until RUES status semantics are normalized.
- First pass excludes common placeholder NITs, requires SECOP exposure, and treats post-inactive contracting as high confidence only when the supplier has inactive RUES evidence and no active RUES row for that exact document.

Score factors:

- +0.25 contract after active cancellation/inactive status.
- +0.20 registration less than 180 days before large award.
- +0.15 contract value above observed capacity proxy.
- +0.15 CIIU/object mismatch.
- +0.10 shared officer or identity inconsistency.
- +0.10 branch/domicile churn.
- +0.10 direct/low competition.

Output:

- `entity_label`: `Company`.
- `scope_key`: `rues_capacity:<supplier_nit>:<status_or_contract>`.
- Evidence: RUES row, contract rows, officer/branch rows where available.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-rues-capacity-status-20260605`, with 1,044 hits and 6,499 evidence rows. Aggregate validation: 1,044 exact suppliers and COP 37.504T total procurement exposure. Severity split: 155 high and 889 medium. Review-type split: 507 recent-registration large-award rows, 434 multi-matricula supplier rows, 73 post-inactive-contract rows, and 35 stale-renewal high-value rows.

Case anchors:

- Centros Poblados bidder capacity concerns: https://www.procuraduria.gov.co/Pages/procuraduria-sanciono-exfuncionarios-mintic-y-union-temporal-centros-poblados-2020.aspx
- OCAD Paz executor capacity review: https://www.procuraduria.gov.co/Pages/investigacion-contra-exmiembros-ocad-paz-por-irregularidades-en-proyecto-financiado-con-regalias-.aspx

## 11. `sgr_ocad_executor_capacity_gap`

Question: did a BPIN/SGR/OCAD project assign large works to weak-capacity executors or contractors, then show low execution or procurement anomalies?

Required sources:

- Loaded and implemented in the first pass: `qkv4-ek54`, `mzgh-shtp`, `d9na-abhe`, SECOP II contracts, RUES, modifications, suspensions, execution, and SECOP sanctions.
- Loaded and implemented in the adjacent DNP extension: `epzv-8ck4` DNP executors, `xikz-44ja` project locations, `iuc2-3r6h` beneficiary locations, and `tmmn-mpqc` beneficiary characterization.

Core keys:

- BPIN.
- Executor entity code/NIT if available.
- Project location/territory.
- Procurement contracts linked by `secop_process_bpin`.

Hard gates:

- Exact BPIN.
- Large project or priority territory.
- Low execution or high advance/payment/modification mismatch.
- Executor/contractor capacity red flag or supplier concentration.

Score factors:

- +0.20 low physical/project-management index or low financial execution.
- +0.20 high advance/paid value versus incomplete works.
- +0.15 executor capacity/CIIU/status concern.
- +0.15 repeated modifications/suspensions.
- +0.10 same supplier concentration.
- +0.10 PDET/PNIS/priority infrastructure context.
- +0.10 official-case bulletin overlap when loaded.

Output:

- `entity_label`: `Project`.
- `entity_key`: BPIN.
- `scope_key`: `sgr_ocad_capacity:<bpin>`.
- Evidence: SGR execution row, BPIN link row, contracts, RUES rows, modifications/suspensions.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-sgr-ocad-capacity-20260605`, with 60 hits and 689 evidence rows. Aggregate validation: 33 executors, COP 2.849T project value, COP 2.581T linked SECOP value, and COP 1.527T SGR payments. Severity split: 27 critical, 16 high, and 17 medium. Flag split: 60 low SGR execution rows, 30 paid-low-physical rows, 39 high-contracting-low-physical rows, 39 procurement-failure rows, 7 RUES supplier-capacity rows, and 12 OCAD Paz/project-paz rows. The DNP beneficiary/location context is implemented as `dnp_sgr_beneficiary_delivery_gap_review_only`.

Case anchors:

- OCAD Paz Miranda capacity and project-management issue: https://www.procuraduria.gov.co/Pages/investigacion-contra-exmiembros-ocad-paz-por-irregularidades-en-proyecto-financiado-con-regalias-.aspx
- OCAD Paz Cauca advance versus incomplete works: https://www.procuraduria.gov.co/Pages/investigacion-por-presuntas-irregularidades-contrato-de-ocad-paz-por-mas-de-13-mil-millones.aspx

## 11A. `dnp_sgr_beneficiary_delivery_gap_review_only`

Question: did an exact-BPIN SGR low-execution/high-procurement project also have DNP executor, location, beneficiary-territory, or vulnerable-population context that should prioritize delivery review?

Required sources:

- Loaded and implemented: `sgr_ocad_executor_capacity_gap`, `epzv-8ck4` DNP executors, `xikz-44ja` project locations, `iuc2-3r6h` beneficiary locations, and `tmmn-mpqc` beneficiary characterization.
- Supporting context: linked SECOP contracts, RUES supplier-capacity flags, suspensions, modifications, execution, and SECOP sanctions through the upstream SGR queue.

Core keys:

- Exact BPIN across SGR, SECOP process links, and DNP project/beneficiary sources.
- DNP executor code, project territory, beneficiary territory, and demographic characteristic.

Hard gates:

- Exact BPIN only.
- Must already be in the SGR capacity-gap queue.
- Require at least one DNP executor/location/beneficiary/demographic context row.
- Keep reviewer-only because DNP context is not direct proof of delivery failure.

Score factors:

- +0.20 vulnerable-population demographic context.
- +0.15 high demographic observation count.
- +0.15 multi-territory beneficiary context.
- +0.15 procurement-failure chain from the upstream SGR queue.
- +0.10 OCAD Paz/project-paz context.
- +0.10 project and linked SECOP value scale.

Output:

- `entity_label`: `Project`.
- `entity_key`: BPIN.
- `scope_key`: `dnp_sgr_beneficiary_delivery:<bpin>`.
- Evidence: SGR capacity-gap feature row, DNP executor row, project-location row, beneficiary-location row, beneficiary-characterization row, and upstream SECOP/RUES/procurement-failure refs.
- `what_is_unproven`: beneficiary harm, non-delivery, incorrect targeting, legal breach, and corrupt intent.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-dnp-sgr-beneficiary-delivery-20260605`, with 60 hits and 1,887 evidence rows. Aggregate validation: COP 2.849T project value, COP 2.581T linked SECOP contract value, COP 1.527T SGR payments, 31 critical rows, 23 high rows, and 6 medium rows. Context split: 48 vulnerable-population rows, 44 high demographic-observation rows, 39 procurement-failure-chain rows, 13 multi-territory beneficiary rows, and 12 OCAD Paz/project-paz rows.

## 12. `secop_interadmin_executor_network_review_only`

Question: did an interadministrative agreement move funds through a special-purpose or mixed executor and into concentrated supplier networks outside demonstrated capacity?

Required sources:

- Loaded and implemented in the first pass: `s484-c9k3` SECOP interadministrative agreements, SECOP contracts, processes/offers, PACO/SECOP sanctions, RUES/company registry, SECOP suppliers, modifications, suspensions, and execution evidence through existing exact-NIT signal queues.
- Needed extensions: BPIN/SGR project bridge, DNP executors, downstream subcontract paths, and official-case bulletin overlay.

Core keys:

- Agreement contract/process ID.
- Origin entity and receiving entity exact NIT.
- Supporting signal-family evidence by recipient NIT.

Hard gates:

- Exact recipient NIT and agreement value at least COP 1B.
- At least two independent support signals.
- Require at least three signal families, or procurement competition plus corporate/sanction evidence, or procurement competition plus execution evidence for a special-executor text match, or execution/sanction evidence on agreement value at least COP 10B.
- Reviewer-only due chain interpretation.

Score factors:

- +0.20 agreement recipient has weak capacity/CIIU mismatch.
- +0.20 downstream supplier concentration.
- +0.15 direct/special regime modality.
- +0.15 BPIN/SGR execution gap.
- +0.10 modifications/suspensions.
- +0.10 role/declaration/donor overlap.
- +0.10 official case/bulletin overlap.

Output:

- `entity_label`: `Company`.
- `scope_key`: `interadmin_chain:<agreement_id>:<recipient_nit>`.
- Evidence: SECOP interadministrative agreement row, plus ranked source refs from the recipient's supporting signal features.

Case anchor: OCAD Paz investigations repeatedly mention special executors and mixed-fund capacity questions; use as review leads, not one-to-one proof.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-interadmin-network-fiscal-20260605`, with 698 hits and 10,357 evidence rows. After guarantee-policy-reuse support refresh, the current table/full run emits 702 hits, including 64 rows with guarantee-policy-reuse support. Original aggregate validation: 252 exact-NIT recipients, 638 agreement IDs, COP 12.673T agreement value, 115 critical rows, 583 high rows, 378 rows with special-executor text, 263 rows with health/PAE service-delivery support, and 74 rows with sanction/fiscal support. Top family combinations are execution failure + procurement competition (310 rows), execution failure + procurement competition + service delivery (235), procurement competition + sanctions (40), corporate capacity + procurement competition (28), and corporate capacity + execution failure + procurement competition (20).

## 13. `health_pae_service_delivery_gap_review_only`

Question: did PAE or health-service contracts show service-delivery gaps, false-service risk, overpricing, or provider mismatch?

Required sources:

- Loaded and implemented in the first pass: `c36g-9fc2` REPS health providers, SECOP contracts, invoices, payment plans, budget-chain context, guarantees, execution, suspensions, modifications, sanctions, and upstream reviewer feature tables.
- Loaded adjacent context: MEN PAE indicators (`epkg-mphw`) by municipality, zone, school day, population group, and beneficiary count.

Core keys:

- Supplier NIT/provider NIT.
- Contract ID and object text.
- Buyer territory.
- Service/beneficiary indicators when loaded.

Hard gates:

- Exact REPS provider NIT match or SECOP object/category text matches PAE, alimentacion escolar, IPS, salud, medicamentos, hemofilia, servicios medicos, ambulancia, vacuna, biomédico, urgencia, tratamiento, or patient/health-service language.
- REPS-only candidates require a health-specific provider class: IPS, special patient transport, or ESE public. Object-social-different providers pass only when SECOP text itself is health or PAE.
- Require at least two independent support signals across at least two support families: payment/budget, execution failure, procurement competition, guarantee/modification/suspension, or sanctions.
- Reviewer-only until service-line, beneficiary, patient, audit, or contract-file evidence is direct.

Score factors:

- +0.20 provider NIT not active/mismatched in REPS for territory/service.
- +0.20 paid/invoiced value high while execution low.
- +0.15 item-price or unit-cost outlier.
- +0.15 repeated additions/modifications.
- +0.10 supervision/role overlap.
- +0.10 beneficiary/coverage mismatch.
- +0.10 sanction/fiscal hit.

Output:

- `entity_label`: `Company`.
- `scope_key`: `health_pae_delivery:<contract_id>`.
- Evidence: contract, REPS row, payment/execution rows, modification/suspension rows, PAE/beneficiary context.
- `what_is_unproven`: false service, overbilling, delivery failure, provider ineligibility, beneficiary mismatch, and corrupt intent.

Case anchors:

- Arauca PAE: https://www.procuraduria.gov.co/Pages/sancionados-cinco-exfuncionarios-gobernacion-arauca-irregularidades-contratacion-pae.aspx
- Cartagena PAE: https://www.procuraduria.gov.co/Pages/sobrecostos-527-millones-contratacion-pae-procuraduria-suspendio-exalcalde-exdirectora-cobertura-educativa-cartagena.aspx
- Cordoba hemophilia: https://www.fiscalia.gov.co/colombia/lucha-contra-corrupcion/condenado-exsecretario-de-salud-de-cordoba-por-facilitar-el-pago-de-falsos-tratamientos-a-pacientes-con-hemofilia-y-von-willebrand/

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-health-pae-delivery-tightened-20260605`, with 639 hits and 2,470 evidence rows. Aggregate validation: 462 entities, 210 buyers, COP 593.973T queued contract value, 380 critical rows, 259 high rows, 208 exact REPS matches, 510 health-keyword rows, and 78 PAE-keyword rows. Domain split is health keyword contract (374 rows), REPS provider health contract (132), PAE school feeding (57), REPS provider contract (55), and health provider plus PAE (21). Support families are payment/budget + procurement competition (361), execution failure + procurement competition (255), execution failure + payment/budget (14), and all three families (9).

## 13A. `pae_beneficiary_territory_delivery_gap_review_only`

Question: did a PAE-keyword contract already in the service-delivery risk queue have MEN municipal beneficiary, population-group, or rural/urban context that should prioritize contract-file review?

Required sources:

- Loaded and implemented: `health_pae_service_delivery_gap_review_only` feature table, `epkg-mphw` MEN PAE indicators, and the same SECOP payment/budget/execution/competition support sources used by the base health/PAE queue.

Core keys:

- Exact supplier NIT/person document from the base service-delivery queue.
- Contract ID and buyer territory from SECOP.
- MEN department/municipality text keys and indicator year.

Hard gates:

- Base row must be PAE-keyword and already pass the health/PAE service-delivery queue.
- Buyer department and municipality must match MEN PAE indicator department and municipality after normalized text-key comparison.
- Prefer same-year MEN indicators; otherwise use the latest prior MEN PAE year available for that municipality.
- Reviewer-only because MEN municipal beneficiary context is not direct contract obligation, delivery, ration-quality, or school-service evidence.

Output:

- `entity_label`: `Company` or `Person`.
- `scope_key`: `pae_beneficiary_territory:<contract_id>`.
- Evidence: base health/PAE feature ref, SECOP/REPS/support refs, and MEN PAE indicator refs.
- `what_is_unproven`: beneficiary mismatch, non-delivery, ration quality failure, overbilling, service failure, legal breach, corrupt intent, and per-contract beneficiary obligation.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-pae-beneficiary-territory-20260605`, with 64 hits and 607 evidence rows. Aggregate validation: 33 critical rows, 31 high rows, 18 same-year MEN matches, and 46 latest-prior MEN matches. Indicator years in the current queue are 2020 (3 rows), 2021 (15), and 2022 (46). Top departments by row count are Cauca (22), Antioquia (11), Casanare (7), Boyaca (5), Atlantico (4), Arauca (3), and Narino (3).

## 13B. `secop_i_legacy_supplier_current_risk_review_only`

Question: did an exact-NIT supplier with material SECOP I historical exposure also appear in the current compound-risk queue across independent signal families?

Required sources:

- Loaded and implemented: `secop_i_historical_processes` (`qddk-cgux`) and `cross_signal_compound_risk_review_only` with its underlying current source families.
- Useful extensions: SECOP I additions/modification companion sources where available, RUES corporate-history continuity, official-case bulletins, and downstream subcontract/project overlays.

Core keys:

- Supplier: exact company NIT from SECOP I contractor document fields.
- Legacy exposure: SECOP I contract/process/adjudication IDs, buyer NIT, signing year/date, values with additions, base values, additions, modality/regime, BPIN/T302/postconflict context where present.
- Current support: exact-NIT compound-risk entity key, current signal IDs, current signal families, and ranked evidence refs.

Hard gates:

- Exact company NIT only; exclude placeholders and all-zero documents.
- SECOP I row must be celebrated/awarded-like and at least COP 100M.
- Supplier rollup must have at least two SECOP I legacy contracts and at least COP 1B legacy value.
- Supplier must already be in current exact-NIT `cross_signal_compound_risk_review_only`.
- Reviewer-only because historical exposure plus current risk convergence does not prove continuity of management, legal disability, legacy irregularity, current irregularity, contract breach, nonperformance, or corrupt intent.

Score factors:

- +0.06 legacy value scale.
- +0.03 material SECOP I additions.
- +0.02 repeated direct/exception modality exposure.
- +0.02 broad legacy buyer count.
- +0.03 current compound-risk row has at least four signal families.
- Critical severity when current base severity is critical and the row has at least four current families, COP 50B+ legacy exposure, or COP 5B+ legacy additions.

Output:

- `entity_label`: `Company`.
- `entity_key`: exact supplier NIT.
- `scope_key`: `secop_i_legacy_current_risk:<supplier_nit>`.
- `scope_type`: `secop_i_legacy_supplier_current_risk`.
- Evidence: current compound-risk feature ref, ranked current support refs, and up to five ranked SECOP I historical evidence refs.
- `what_is_unproven`: management continuity, legal disability, legacy/current irregularity, breach, nonperformance, and corrupt intent.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-secop-i-legacy-current-risk-20260605`, with 549 hits and 13,949 evidence rows. After guarantee-policy-reuse support refresh, the current table/full run emits 556 hits, including 11 rows with guarantee-policy-reuse support. Original aggregate validation: 154 critical rows, 395 high rows, COP 77.083T legacy SECOP I contract value, COP 10.912T additions, 15,960 legacy contracts, 1,593 material-addition rows, and 10,754 direct/exception-modality rows. Current support distribution before the refresh was 7 rows with five signal families, 81 with four, and 461 with three.

## 13C. `secop_i_legacy_representative_current_risk_review_only`

Question: did a SECOP I historical legal representative bridge into a different current RUES company that already appears in current compound-risk evidence?

Required sources:

- Loaded and implemented: `secop_i_historical_processes` (`qddk-cgux`), `company_registry_c82u`, and `cross_signal_compound_risk_review_only` with its underlying current source families.
- Useful extensions: RUES historical role-date records, beneficial ownership, official case bulletins, SECOP I contract-file samples, and corporate-history/subcontractor overlays.

Core keys:

- Historical person: exact SECOP I `legal_rep_id` canonicalized as a cedula/person document.
- Historical exposure: SECOP I contractor NIT, contract/process/adjudication IDs, buyer NIT, signing year/date, values with additions, additions value, modality/regime, BPIN/T302/postconflict context where present.
- Current company bridge: exact RUES legal-representative document to exact company NIT.
- Current support: exact-NIT compound-risk company row and ranked current evidence refs.

Hard gates:

- Exact person document on SECOP I legal representative and RUES current legal representative.
- SECOP I contractor must be a canonical NIT; SECOP I row must be celebrated and at least COP 100M.
- Representative rollup must have at least COP 5B SECOP I legacy value.
- Require material additions of at least COP 500M, at least five direct/exception contracts, or at least ten legacy contracts.
- Current RUES company must already be in exact-NIT `cross_signal_compound_risk_review_only` with at least three current signal families.
- Exclude rows where the current company NIT is one of the representative's SECOP I legacy contractor NITs, so this detector stays focused on person-mediated hidden bridges.
- Bound representative current-risk company count to at most five.
- Reviewer-only because the bridge does not prove ownership, beneficial control, legal disability, legacy irregularity, current irregularity, breach, nonperformance, or corrupt intent.

Score factors:

- +0.06 legacy value scale.
- +0.03 material SECOP I additions.
- +0.03 repeated direct/exception modality exposure.
- +0.02 broad legacy buyer count.
- +0.03 current compound-risk row has at least four signal families.
- +0.02 representative bridges to one or two current-risk companies only.
- Critical severity when current base severity is critical and the row has at least four current families, COP 50B+ legacy exposure, COP 1B+ legacy additions, or at least ten direct/exception legacy rows; always critical at COP 100B+ legacy exposure.

Output:

- `entity_label`: `Company`.
- `entity_key`: exact current company NIT.
- `scope_key`: `secop_i_legacy_representative_current_risk:<representative_doc>:<current_company_nit>`.
- `scope_type`: `secop_i_legacy_representative_current_risk`.
- Evidence: current compound-risk feature ref, ranked current support refs, up to five ranked SECOP I historical evidence refs, and current RUES company registry ref.
- Flags: exact representative bridge, different legacy contractor/current company, high legacy value, additions/direct-or-repeated exposure, current compound risk, and bounded current-company bridge.
- `what_is_unproven`: ownership, beneficial control, legal disability, legacy/current irregularity, breach, nonperformance, and corrupt intent.

Implementation status: wired in ETL/registry and materialized locally in run `phase-missing-patterns-secop-i-representative-current-risk-20260606`, with 13 hits and 338 evidence rows; after shared-representative same-buyer support refresh the current table/full run emits 14 hits. Aggregate selected-run validation: 12 representative documents, 13 current companies, 8 critical rows, 5 high rows, COP 361.52B SECOP I legacy value, COP 45.39B additions, and all rows satisfying the exact bridge, different-company, high-value, additions/direct-or-repeated, current compound-risk, and bounded-bridge flags. Four refreshed rows now carry `procurement_shared_representative_same_buyer_cluster_review_only` support.

## Cross-detector hidden connections

These connections should be computed as a second-stage reviewer graph after individual feature tables exist:

| connection | why it matters | join keys |
|---|---|---|
| role supplier + declaration chronology | strongest person-level conflict queue | person document, buyer NIT, contract date |
| declaration company bridge + current compound risk | public/private-interest person connected through RUES representative role to a multi-family current-risk supplier company | person document, company NIT, RUES matricula, current signal-family refs |
| donor ineligibility + declaration chronology | campaign finance plus public/private interest | donor/declarant document, candidate jurisdiction |
| TVEC overprice + supplier capture | overpricing by supplier already broadly embedded in state purchasing | supplier NIT, buyer NIT |
| guarantee/advance chain + SGR/OCAD gap | money paid early, weak execution, public-project harm | contract ID, BPIN |
| budget-chain reconciliation + guarantee/advance chain | weak budget support plus execution/guarantee risk on the same exact contract | contract ID |
| invoice-budget reconciliation + budget/guarantee chain | invoice value or timing conflicts with contract, budget, or execution context | contract ID |
| payment-plan reconciliation + invoice/budget/guarantee chain | real-payment timing/value/CUFE contradictions layered on budget, invoice, or execution risk | contract ID, CUFE, commitment reference |
| modification ladder + budget/invoice/guarantee/competition support | repeated contractual changes plus independent execution, payment, budget, or competition red flags | contract ID, supplier NIT, buyer NIT |
| related bidders same process + shared officer/co-bidding | apparent competition between companies with a shared representative inside the same exact SECOP process | process ID, winner NIT, offerer NIT, RUES representative document |
| shared representative same buyer + cross-signal risk | multiple supplier companies tied to one representative and one buyer/year, then reinforced by execution, capacity, sanctions, or legacy evidence | representative document, buyer NIT, supplier NIT, signing year |
| RUES capacity + interadmin chain | weak/special entity as conduit or executor | supplier/recipient NIT, agreement ID, BPIN |
| DNP beneficiary delivery + SGR/OCAD gap | beneficiary/location/demographic context on low-execution, high-procurement royalty projects | BPIN |
| PAE beneficiary territory + payment/budget/execution risk | vulnerable-service beneficiary context on exact PAE delivery-risk contracts | supplier NIT/document, contract ID, buyer municipality |
| SIRI antecedent + role supplier | legal/disciplinary history plus direct contract-role conflict | person document, contract date |
| fiscal chronology + later awards/cross-signal | official fiscal finding or responsibility record plus later procurement exposure and independent support | subject NIT/document, contract date |
| PAE/health delivery + TVEC overprice | concrete service/goods overpricing in vulnerable-service domains | supplier NIT, item/category, buyer territory |
| SECOP I legacy exposure + current compound risk | time-spanning supplier continuity from historical exposure to present-day independent risk-family convergence | supplier NIT, SECOP I record ID, current signal-family refs |
| SECOP I legal representative + current compound risk | person-mediated continuity from historical procurement exposure into a different current RUES company with independent risk-family convergence | representative document, SECOP I record ID, current company NIT, current signal-family refs |
| official case bulletin + any detector | public validation and safer public reporting | normalized names, NIT/doc, BPIN, contract ID |

Implementation status: `cross_signal_compound_risk_review_only` is now wired in ETL/registry and refreshed after the modification-ladder, related-bidders, guarantee-policy-reuse, and shared-representative same-buyer detectors. The capped output remains 1,000 rows; 126 rows carry `procurement_contract_modification_ladder_review_only` support, 3 rows carry `procurement_related_bidders_same_process_review_only` support, 43 rows carry `procurement_guarantee_policy_reuse_review_only` support, and 26 rows carry `procurement_shared_representative_same_buyer_cluster_review_only` support. The DNP/BPIN rows remain project-family-only while the current cross gate prioritizes multi-family company/person convergence.

## Registry additions to consider

All entries should start with `public_safe: false` and `reviewer_only: true` until legal-status parsing, identity policy, and `what_is_unproven` text are implemented. `procurement_role_supplier_same_buyer_review_only`, `public_declaration_supplier_chronology_review_only`, `public_declaration_company_bridge_current_risk_review_only`, `tvec_item_price_dispersion_review_only`, `cuentas_claras_donor_ineligibility_review`, `procurement_secop_sanction_later_awards_review_only`, `fiscal_procurement_chronology_review_only`, `siri_antecedent_procurement_chronology_review_only`, `procurement_guarantee_advance_execution_chain`, `procurement_guarantee_policy_reuse_review_only`, `procurement_budget_chain_reconciliation_review_only`, `procurement_invoice_budget_reconciliation_review_only`, `procurement_payment_plan_reconciliation_review_only`, `procurement_contract_modification_ladder_review_only`, `procurement_related_bidders_same_process_review_only`, `procurement_shared_representative_same_buyer_cluster_review_only`, `health_pae_service_delivery_gap_review_only`, `pae_beneficiary_territory_delivery_gap_review_only`, `rues_supplier_capacity_status_review_only`, `sgr_ocad_executor_capacity_gap`, `dnp_sgr_beneficiary_delivery_gap_review_only`, `cross_signal_compound_risk_review_only`, `secop_i_legacy_supplier_current_risk_review_only`, `secop_i_legacy_representative_current_risk_review_only`, and `secop_interadmin_executor_network_review_only` have already been added to the registry, ETL materialization path, and local lake; the rest remain proposed.

| signal id | category | severity | entity types | sources required |
|---|---|---|---|---|
| `procurement_role_supplier_same_buyer_review_only` | conflict | high | `Person` | `secop_ii_contracts` |
| `public_declaration_supplier_chronology_review_only` | conflict | high | `Person` | `asset_disclosures`, `conflict_disclosures`, `sigep_sensitive_positions`, `secop_ii_contracts` |
| `public_declaration_company_bridge_current_risk_review_only` | conflict | high | `Company` | declaration/SIGEP/RUES sources plus cross-signal source families |
| `tvec_item_price_dispersion_review_only` | procurement | high | `Company`, `ProcurementItem` | `tvec_orders_consolidated` |
| `cuentas_claras_donor_ineligibility_review` | election | medium | `Company`, `Person` | `cuentas_claras_income_2019`, `secop_ii_contracts` |
| `procurement_secop_sanction_later_awards_review_only` | sanctions | high | `Company`, `Person` | `secop_sanctions`, `secop_ii_contracts` |
| `fiscal_procurement_chronology_review_only` | sanctions | high | `Company`, `Person` | `fiscal_findings`, `fiscal_responsibility`, `secop_ii_contracts` |
| `siri_antecedent_procurement_chronology_review_only` | sanctions | high | `Person`, `Company` | `siri_antecedents`, `secop_ii_contracts`, `company_registry_c82u`, `secop_suppliers` |
| `procurement_guarantee_advance_execution_chain` | procurement | high | `Company`, `Person` | `secop_ii_contracts`, `secop_guarantees`, `secop_contract_execution`, `secop_contract_suspensions`, `secop_contract_modifications`, `secop_sanctions` |
| `procurement_guarantee_policy_reuse_review_only` | procurement | high | `Company`, `Person` | `secop_ii_contracts`, `secop_guarantees` |
| `procurement_budget_chain_reconciliation_review_only` | procurement | high | `Company`, `Person` | `secop_ii_contracts`, `secop_cdp_requests`, `secop_budget_commitments`, `secop_budget_items`, `secop_guarantees`, `secop_contract_execution`, `secop_contract_suspensions`, `secop_contract_modifications`, `secop_sanctions` |
| `procurement_invoice_budget_reconciliation_review_only` | procurement | high | `Company`, `Person` | `secop_ii_contracts`, `secop_invoices`, `secop_cdp_requests`, `secop_budget_commitments`, `secop_budget_items`, `secop_guarantees`, `secop_contract_execution`, `secop_contract_suspensions`, `secop_contract_modifications`, `secop_sanctions` |
| `procurement_payment_plan_reconciliation_review_only` | procurement | high | `Company`, `Person` | `secop_ii_contracts`, `secop_payment_plans`, `secop_invoices`, `secop_cdp_requests`, `secop_budget_commitments`, `secop_budget_items`, `secop_guarantees`, `secop_contract_execution`, `secop_contract_suspensions`, `secop_contract_modifications`, `secop_sanctions` |
| `procurement_contract_modification_ladder_review_only` | procurement | high | `Company`, `Person` | `secop_ii_contracts`, `secop_contract_modifications`, `secop_contract_suspensions`, `secop_contract_execution` |
| `procurement_related_bidders_same_process_review_only` | procurement | high | `Company` | `secop_offers`, `secop_ii_contracts`, `company_registry_c82u` |
| `procurement_shared_representative_same_buyer_cluster_review_only` | procurement | high | `Company` | `secop_ii_contracts`, `company_registry_c82u` |
| `rues_supplier_capacity_status_review_only` | procurement | high | `Company` | `company_registry_c82u`, `secop_ii_contracts` |
| `sgr_ocad_executor_capacity_gap` | projects | high | `Project` | `sgr_expense_execution`, `sgr_projects`, `secop_process_bpin`, `secop_ii_contracts`, `company_registry_c82u`, `secop_contract_suspensions`, `secop_contract_modifications`, `secop_contract_execution`, `secop_sanctions` |
| `dnp_sgr_beneficiary_delivery_gap_review_only` | projects | high | `Project` | `sgr_expense_execution`, `sgr_projects`, `secop_process_bpin`, `secop_ii_contracts`, `company_registry_c82u`, `dnp_project_executors`, `dnp_project_locations`, `dnp_project_beneficiary_locations`, `dnp_project_beneficiary_characterization` |
| `cross_signal_compound_risk_review_only` | cross_detector | high | `Company`, `Person`, `Project` | materialized signal feature tables plus their source families |
| `secop_i_legacy_supplier_current_risk_review_only` | procurement | high | `Company` | `secop_i_historical_processes` plus cross-signal source families |
| `secop_i_legacy_representative_current_risk_review_only` | procurement | high | `Company` | `secop_i_historical_processes`, `company_registry_c82u`, plus cross-signal source families |
| `secop_interadmin_executor_network_review_only` | procurement | high | `Company` | `secop_interadmin_agreements`, `secop_ii_contracts`, `company_registry_c82u`, `health_providers`, `secop_contract_suspensions`, `secop_contract_modifications`, `secop_contract_execution`, `secop_sanctions` |
| `health_pae_service_delivery_gap_review_only` | services | high | `Company` | `secop_ii_contracts`, `health_providers`, `secop_payment_plans`, `secop_invoices`, `secop_cdp_requests`, `secop_budget_commitments`, `secop_budget_items`, `secop_guarantees`, `secop_contract_execution`, `secop_contract_suspensions`, `secop_contract_modifications`, `secop_sanctions` |
| `pae_beneficiary_territory_delivery_gap_review_only` | services | high | `Company`, `Person` | `health_providers`, `pae_indicators`, `secop_ii_contracts`, `secop_payment_plans`, `secop_invoices`, `secop_cdp_requests`, `secop_budget_commitments`, `secop_budget_items`, `secop_guarantees`, `secop_contract_execution`, `secop_contract_suspensions`, `secop_contract_modifications`, `secop_sanctions` |

## First implementation recommendation

Extend the twenty-five materialized reviewer queues, then map the next missing source family:

1. Review sampled output from `procurement_role_supplier_same_buyer_review_only`, now materialized locally in run `phase-missing-patterns-role-supplier-20260605`; calibrate person-document false positives before public exposure.
2. Review sampled output from `public_declaration_supplier_chronology_review_only`, now materialized locally in run `phase-missing-patterns-declaration-supplier-20260605`; calibrate the 25,000-row cap and same-entity matching.
3. Review sampled output from `public_declaration_company_bridge_current_risk_review_only`, now materialized locally in run `phase-missing-patterns-declaration-company-bridge-current-risk-20260605`; calibrate RUES role-date semantics, public-entity representative cases, and beneficial-control assumptions before public use.
4. Review sampled output from `tvec_item_price_dispersion_review_only`, now materialized locally in run `phase-missing-patterns-tvec-price-20260605`; calibrate item normalization and generic-bundle exclusions.
5. Review sampled output from `cuentas_claras_donor_ineligibility_review`, now materialized locally in run `phase-missing-patterns-cuentas-ineligibility-20260605`; load election-result and campaign-cap inputs before any legal classification.
6. Review sampled output from `procurement_secop_sanction_later_awards_review_only`, now materialized locally in run `phase-missing-patterns-secop-sanction-later-awards-20260605`; calibrate sanction status handling and join to same-buyer role/declaration/donor queues.
7. Review sampled output from `fiscal_procurement_chronology_review_only`, now materialized locally in run `phase-missing-patterns-fiscal-procurement-chronology-v2-20260605`; calibrate legal finality, current-disability semantics, public-entity subject rows, and cross-signal support before any public use.
8. Review sampled output from `siri_antecedent_procurement_chronology_review_only`, now materialized locally in run `phase-missing-patterns-siri-antecedent-20260605`; calibrate inferred active-period handling, legal finality text, and donor/declaration/fiscal cross-support before any public use.
9. Review sampled output from `procurement_guarantee_advance_execution_chain`, now materialized locally in run `phase-missing-patterns-guarantee-advance-chain-v2-20260605`; calibrate guarantee status/date/value semantics before any guarantee-validity statement.
10. Review sampled output from `procurement_guarantee_policy_reuse_review_only`, now materialized locally in run `phase-missing-patterns-guarantee-policy-reuse-20260605`; calibrate insurer-confirmation workflow, endorsements, and legitimate master-policy explanations before any guarantee-validity statement.
11. Review sampled output from `procurement_budget_chain_reconciliation_review_only`, now materialized locally in run `phase-missing-patterns-budget-chain-20260605`; calibrate source-value outliers and weak SIIF/failed-integration semantics before any budget-irregularity statement.
12. Review sampled output from `procurement_invoice_budget_reconciliation_review_only`, now materialized locally in run `phase-missing-patterns-invoice-budget-reconciliation-20260605`; calibrate invoice status/value semantics before any payment-irregularity statement.
13. Review sampled output from `procurement_payment_plan_reconciliation_review_only`, now materialized locally in run `phase-missing-patterns-payment-plan-reconciliation-20260605`; calibrate payment status, CUFE reuse, supplier-document mismatch, and real-payment timing before any payment-irregularity statement.
14. Review sampled output from `procurement_contract_modification_ladder_review_only`, now materialized locally in run `phase-missing-patterns-modification-ladder-20260605`; calibrate modification-value/version semantics, legal effect of otrosies, and contract-file support before any statement about unlawful modification or competition avoidance.
15. Review sampled output from `procurement_related_bidders_same_process_review_only`, now materialized locally in run `phase-missing-patterns-related-bidders-20260605`; calibrate shared-representative business-group cases, insurance/consortium structures, and broad-tender retention before any statement about simulated competition or collusion.
16. Review sampled output from `procurement_shared_representative_same_buyer_cluster_review_only`, now materialized locally in run `phase-missing-patterns-shared-representative-same-buyer-20260606`; calibrate buyer-market concentration, shared-representative business-group explanations, role-date semantics, and beneficial-control assumptions before any statement about coordinated awards.
17. Review sampled output from `rues_supplier_capacity_status_review_only`, now materialized locally in run `phase-missing-patterns-rues-capacity-status-20260605`; calibrate status-event handling and add branch/CIIU/financial-capacity extensions.
18. Review sampled output from `sgr_ocad_executor_capacity_gap`, now materialized locally in run `phase-missing-patterns-sgr-ocad-capacity-20260605`; calibrate project-priority flags and financial-capacity extensions.
19. Review sampled output from `dnp_sgr_beneficiary_delivery_gap_review_only`, now materialized locally in run `phase-missing-patterns-dnp-sgr-beneficiary-delivery-20260605`; calibrate vulnerable-population, high-observation, and multi-territory beneficiary context before any delivery or harm claim.
20. Review sampled output from `health_pae_service_delivery_gap_review_only`, now materialized locally in run `phase-missing-patterns-health-pae-delivery-tightened-20260605`; calibrate health/PAE text gates, REPS provider-class gates, and direct service/beneficiary follow-up.
21. Review sampled output from `pae_beneficiary_territory_delivery_gap_review_only`, now materialized locally in run `phase-missing-patterns-pae-beneficiary-territory-20260605`; calibrate same-year/prior-year MEN joins and direct school/service follow-up before any beneficiary mismatch or non-delivery statement.
22. Review sampled output from `cross_signal_compound_risk_review_only`, now refreshed after `phase-missing-patterns-all-signals-shared-representative-20260606`; calibrate family-combo gates and use it as a case-prioritization layer over individual queues while noting that project-family-only BPIN/DNP rows do not enter the capped company/person output.
23. Review sampled output from `secop_i_legacy_supplier_current_risk_review_only`, now materialized locally in run `phase-missing-patterns-secop-i-legacy-current-risk-20260605` and refreshed after shared-representative same-buyer support; calibrate SECOP I source-value/addition semantics, corporate-history continuity, and current family-combo gates before public use.
24. Review sampled output from `secop_i_legacy_representative_current_risk_review_only`, now materialized locally in run `phase-missing-patterns-secop-i-representative-current-risk-20260606` and refreshed after shared-representative same-buyer support; calibrate RUES role-date semantics, different-company bridge cases, corporate-history continuity, and beneficial-control assumptions before public use.
25. Review sampled output from `secop_interadmin_executor_network_review_only`, now refreshed after guarantee-policy reuse; calibrate special-executor text, exact-NIT support-family gates, service-delivery/fiscal support, and downstream subcontract/BPIN/SGR linkage.

Then load or map the next three source families in this order:

1. Direct school/service evidence and health service-line authorization extensions beyond municipal MEN PAE context.
2. Official case bulletins, beneficial-ownership, or subcontractor sources to harden hidden-connection triage.
3. RUES branch/CIIU and company financial-capacity sources for stronger supplier and SGR/interadministrative capacity scoring.
