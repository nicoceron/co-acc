# High-confidence missing corruption patterns

Generated: 2026-06-05

Scope: Colombia-focused review of the local lake, configured signal registry, SQL candidate signals, and public case anchors. This is a triage artifact, not an allegation list. Candidate patterns below should produce review queues with exact public evidence and clear false-positive controls.

Implementation companion: `docs/reviews/missing_corruption_pattern_detector_blueprints_2026-06-05.md` turns the top missing patterns into detector IDs, keys, hard gates, score factors, output fields, and registry additions.

## Current materialized baseline

The local lake currently has 49 materialized signal feature tables after adding reviewer-only role/supplier, declaration/supplier chronology, declaration-company bridge, TVEC item-price dispersion, Cuentas Claras donor-contract ineligibility-review, SECOP sanction later-awards, fiscal finding/responsibility procurement chronology, SIRI antecedent/procurement chronology, guarantee/advance/execution-chain, guarantee-policy reuse, budget-chain reconciliation, invoice-budget reconciliation, payment-plan reconciliation, contract modification-ladder, related-bidders same-process, shared-representative same-buyer clusters, health/PAE service-delivery, PAE beneficiary-territory context, RUES supplier-capacity/status, SGR/OCAD executor-capacity gap, DNP/SGR beneficiary-delivery context, cross-signal compound-risk, SECOP I legacy supplier/current-risk bridge, SECOP I legacy representative/current-risk bridge, and SECOP interadministrative executor-network detectors. The dominant coverage is still procurement mechanics plus a few Colombia-specific joins.

| materialized signal | rows | main scope | note |
|---|---:|---|---|
| `procurement_short_bidding_window` | 90,592 | procurement process | broad pre-award timing proxy |
| `procurement_single_bidder_high_value` | 61,542 | procurement process | low competition |
| `public_declaration_supplier_chronology_review_only` | 25,000 | declaration/supplier | reviewer-only exact declarant or sensitive-position document to later supplier exposure |
| `procurement_sanctioned_supplier_awarded` | 20,184 | sanction record | PACO/fiscal/sanction overlap |
| `procurement_role_supplier_same_buyer_review_only` | 11,311 | buyer role/supplier | reviewer-only SECOP role document to same-buyer supplier overlap |
| `procurement_contract_suspensions` | 10,460 | contract | repeated suspension events |
| `procurement_repeat_awards_same_supplier` | 9,716 | buyer | repeat buyer-supplier awards |
| `tvec_item_price_dispersion_review_only` | 4,601 | TVEC price comparator | reviewer-only item/unit/quantity/year p95 outliers |
| `procurement_contract_value_outlier_by_category` | 2,022 | contract | reviewer-only value outliers |
| `procurement_related_companies_shared_officer` | 1,482 | officer cluster | reviewer-only RUES/contract overlap |
| `siri_antecedent_procurement_chronology_review_only` | 1,120 | SIRI antecedent/procurement chronology | reviewer-only exact person document to supplier, contract-role, or legal-representative procurement exposure |
| `rues_supplier_capacity_status_review_only` | 1,044 | RUES supplier capacity/status | reviewer-only exact NIT status, renewal, registration-age, and multiple-registration review queue |
| `procurement_budget_chain_reconciliation_review_only` | 1,000 | contract budget chain | reviewer-only CDP/commitment/rubro support gaps or mismatches, prioritized when guarantee/execution-chain evidence is present |
| `procurement_payment_plan_reconciliation_review_only` | 1,000 | contract payment-plan reconciliation | reviewer-only exact contract real-payment overruns, post-end payments, invoice-sequence issues, duplicate CUFE, or supplier-document mismatches |
| `procurement_cartel_risk_cobidding` | 866 | co-bid cluster | reviewer-only co-bidding proxy |
| `procurement_related_bidders_same_process_review_only` | 16 | related bidders process | reviewer-only exact SECOP winner/competing-offerer pair with shared RUES representative |
| `procurement_shared_representative_same_buyer_cluster_review_only` | 166 | shared representative/buyer/year cluster | reviewer-only exact RUES representative with multiple supplier companies awarded by the same buyer in one year |
| `procurement_guarantee_policy_reuse_review_only` | 723 | guarantee policy cluster | reviewer-only exact insurer/policy reuse across different suppliers and buyers |
| `bpin_dnp_vs_pida27_obras_prioritarias` | 796 | project | BPIN priority-work awards |
| `project_bpin_procurement_overlap` | 654 | project | BPIN to SECOP contract links |
| `procurement_large_modifications` | 582 | contract | high-value modifications |
| `procurement_contract_modification_ladder_review_only` | 320 | contract | reviewer-only repeated SECOP modification sequence with value/delay/payment/scope/support convergence |
| `cross_signal_compound_risk_review_only` | 1,000 | cross-signal entity | reviewer-only exact identity convergence across independent signal families |
| `secop_i_legacy_supplier_current_risk_review_only` | 556 | SECOP I legacy/current supplier | reviewer-only exact-NIT SECOP I historical supplier exposure joined to current compound-risk entities |
| `secop_i_legacy_representative_current_risk_review_only` | 14 | SECOP I legal representative/current company | reviewer-only exact SECOP I legal representative document joined through current RUES legal representation to current compound-risk companies |
| `secop_interadmin_executor_network_review_only` | 702 | interadministrative agreement recipient | reviewer-only high-value SECOP interadministrative recipients with exact-NIT overlap in independent procurement/capacity/execution/sanction/service-delivery queues |
| `health_pae_service_delivery_gap_review_only` | 639 | health/PAE service-delivery | reviewer-only exact REPS provider or SECOP health/PAE text plus independent payment, budget, execution, modification, suspension, guarantee, or competition support |
| `pae_beneficiary_territory_delivery_gap_review_only` | 64 | PAE beneficiary territory | reviewer-only MEN municipal PAE beneficiary context added to PAE-keyword contracts already in the service-delivery risk queue |
| `public_declaration_company_bridge_current_risk_review_only` | 58 | declaration/RUES/company bridge | reviewer-only exact public declarant or sensitive-position person to RUES legal-representative company with current compound-risk evidence |
| `cuentas_claras_donor_supplier_overlap` | 533 | election | broad donor/supplier overlap |
| `procurement_invoice_budget_reconciliation_review_only` | 376 | contract invoice/budget chain | reviewer-only exact contract invoice overrun, post-end invoice, or summary mismatch with budget/guarantee context |
| `procurement_payment_plan_anomalies` | 375 | contract | payment cadence/concentration |
| `cuentas_claras_donor_ineligibility_review` | 322 | donor contract review | same-jurisdiction 2019 local campaign donor contracts, with legal cap/result missing |
| `procurement_guarantee_advance_execution_chain` | 257 | contract execution chain | reviewer-only advance/pending-execution plus suspension/modification/delay/sanction chain, now with SECOP guarantee status/date/value evidence |
| `procurement_secop_sanction_later_awards_review_only` | 127 | sanction chronology | reviewer-only SECOP sanction-contract to exact later supplier awards |
| `fiscal_procurement_chronology_review_only` | 80 | fiscal/procurement chronology | reviewer-only exact Contraloria fiscal finding or fiscal-responsibility NIT to later SECOP supplier exposure |
| `procurement_public_servant_conflict_disclosure_overlap` | 65 | disclosure | public servant conflict disclosure plus supplier exposure |
| `sgr_ocad_executor_capacity_gap` | 60 | SGR/OCAD project | reviewer-only BPIN/SGR low execution plus high procurement, RUES, or procurement-failure flags |
| `dnp_sgr_beneficiary_delivery_gap_review_only` | 60 | DNP/SGR project | reviewer-only exact BPIN DNP executor/location/beneficiary context layered on the SGR capacity-gap queue |
| `project_regalias_execution_procurement_overlap` | 41 | project | SGR execution plus procurement |
| `pida5_pida27_pida4_chain` | 38 | territory | sanctioned supplier on priority infrastructure territory |

Local source coverage manifests show these high-value raw sources are loaded: SECOP Integrado (`rpmr-utcd`, 21,869,971 rows), SECOP II offers (`wi7w-2nvm`, 42,264,321 rows), SECOP II processes (`p6dx-8zbt`, 8,648,158 rows), SECOP II contracts (`jbjy-vk9h`, 5,614,448 rows), SECOP I historical (`qddk-cgux`, 6,123,394 rows), SECOP interadministrative agreements active mirror (`s484-c9k3`, 88,522 rows), REPS health providers (`c36g-9fc2`, 76,821 rows), MEN PAE indicators (`epkg-mphw`, 121,379 rows), SECOP guarantees (`gjp9-cutm`, 6,705,690 rows), SECOP invoices (`ibyt-yi2f`, 21,002,798 rows), SECOP payment plans (`uymx-8p3j`, 17,480,966 rows), SECOP CDP requests (`a86w-fh92`, 9,572,873 rows), SECOP budget commitments (`skc9-met7`, 5,866,580 rows), SECOP budget items (`cwhv-7fnp`, 5,891,594 rows), SIRI antecedents (`iaeu-rcn6`, 43,491 rows), Hallazgos Fiscales (`8qxx-ubmq`, 73 rows), Responsabilidad Fiscal (`jr8e-e8tu`, 60 rows), RUES company registry (`c82u-588k`, 9,264,493 rows), asset disclosures (`8tz7-h3eu`, 330,006 rows), conflict disclosures (`gbry-rnq4`, 328,799 rows), Cuentas Claras income 2019 (`jgra-rz2t`, 188,171 rows), SGR execution (`qkv4-ek54`, 386,293 rows), SGR projects (`mzgh-shtp`, 35,006 rows), SECOP BPIN links (`d9na-abhe`, 2,450,545 rows), DNP project executors (`epzv-8ck4`, 315,191 rows), DNP project locations (`xikz-44ja`, 735,326 rows), DNP beneficiary locations (`iuc2-3r6h`, 649,442 rows), DNP beneficiary characterization (`tmmn-mpqc`, 1,683,923 rows), TVEC item purchases (`3hdv-smhz`, 1,400,581 rows), PACO sanctions (54,369 rows), SECOP sanctions (`it5q-hg94`, 538 rows), SECOP execution (`mfmm-jqmq`, 9,535 rows), modifications (`u8cx-r425`, 63,000 rows), and suspensions (`u99c-7mfm`, 537,314 rows).

Observed audit frame:

- `config/signal_registry.yml` now defines 68 signals after adding `procurement_role_supplier_same_buyer_review_only`, `public_declaration_supplier_chronology_review_only`, `public_declaration_company_bridge_current_risk_review_only`, `tvec_item_price_dispersion_review_only`, `cuentas_claras_donor_ineligibility_review`, `procurement_secop_sanction_later_awards_review_only`, `fiscal_procurement_chronology_review_only`, `siri_antecedent_procurement_chronology_review_only`, `procurement_guarantee_advance_execution_chain`, `procurement_guarantee_policy_reuse_review_only`, `procurement_budget_chain_reconciliation_review_only`, `procurement_invoice_budget_reconciliation_review_only`, `procurement_payment_plan_reconciliation_review_only`, `procurement_contract_modification_ladder_review_only`, `procurement_related_bidders_same_process_review_only`, `procurement_shared_representative_same_buyer_cluster_review_only`, `health_pae_service_delivery_gap_review_only`, `pae_beneficiary_territory_delivery_gap_review_only`, `rues_supplier_capacity_status_review_only`, `sgr_ocad_executor_capacity_gap`, `dnp_sgr_beneficiary_delivery_gap_review_only`, `cross_signal_compound_risk_review_only`, `secop_i_legacy_supplier_current_risk_review_only`, `secop_i_legacy_representative_current_risk_review_only`, and `secop_interadmin_executor_network_review_only`; 49 have local curated feature tables.
- The new role/supplier detector was materialized locally in run `phase-missing-patterns-role-supplier-20260605`, producing 11,311 hits and 68,270 evidence rows.
- The new declaration/supplier chronology detector was materialized locally in run `phase-missing-patterns-declaration-supplier-20260605`, producing 25,000 hits and 135,488 evidence rows.
- The new declaration-company bridge detector was materialized locally in run `phase-missing-patterns-declaration-company-bridge-current-risk-20260605`, producing 56 hits and 1,354 evidence rows.
- The new TVEC item-price detector was materialized locally in run `phase-missing-patterns-tvec-price-20260605`, producing 4,601 hits and 13,877 evidence rows.
- The new Cuentas donor-contract ineligibility-review detector was materialized locally in run `phase-missing-patterns-cuentas-ineligibility-20260605`, producing 322 hits and 1,220 evidence rows.
- The new SECOP sanction later-awards detector was materialized locally in run `phase-missing-patterns-secop-sanction-later-awards-20260605`, producing 127 hits and 588 evidence rows.
- The new fiscal finding/responsibility procurement chronology detector was materialized locally in run `phase-missing-patterns-fiscal-procurement-chronology-v2-20260605`, producing 80 hits and 400 evidence rows.
- The new SIRI antecedent/procurement chronology detector was materialized locally in run `phase-missing-patterns-siri-antecedent-20260605`, producing 1,120 hits and 2,357 evidence rows.
- The upgraded guarantee/advance/execution-chain detector was materialized locally in run `phase-missing-patterns-guarantee-advance-chain-v2-20260605`, producing 257 hits and 1,505 evidence rows, including 704 SECOP guarantee evidence rows.
- The new guarantee-policy reuse detector was materialized locally in run `phase-missing-patterns-guarantee-policy-reuse-20260605`, producing 723 hits and 3,110 evidence rows.
- The new budget-chain reconciliation detector was materialized locally in run `phase-missing-patterns-budget-chain-20260605`, producing 1,000 hits and 3,784 evidence rows.
- The new invoice-budget reconciliation detector was materialized locally in run `phase-missing-patterns-invoice-budget-reconciliation-20260605`, producing 376 hits and 1,603 evidence rows.
- The new payment-plan reconciliation detector was materialized locally in run `phase-missing-patterns-payment-plan-reconciliation-20260605`, producing 1,000 hits and 3,934 evidence rows.
- The new health/PAE service-delivery detector was materialized locally in run `phase-missing-patterns-health-pae-delivery-tightened-20260605`, producing 639 hits and 2,470 evidence rows.
- The new PAE beneficiary-territory detector was materialized locally in run `phase-missing-patterns-pae-beneficiary-territory-20260605`, producing 64 hits and 607 evidence rows.
- The new RUES supplier-capacity/status detector was materialized locally in run `phase-missing-patterns-rues-capacity-status-20260605`, producing 1,044 hits and 6,499 evidence rows.
- The new SGR/OCAD executor-capacity gap detector was materialized locally in run `phase-missing-patterns-sgr-ocad-capacity-20260605`, producing 60 hits and 689 evidence rows.
- The new DNP/SGR beneficiary-delivery context detector was materialized locally in run `phase-missing-patterns-dnp-sgr-beneficiary-delivery-20260605`, producing 60 hits and 1,887 evidence rows.
- The refreshed cross-signal compound-risk detector was materialized locally in run `phase-missing-patterns-cross-signal-compound-pae-beneficiary-20260605`, producing 1,000 hits and 18,791 evidence rows. The DNP project rows did not enter the capped cross-signal output because the cross gate still prioritizes multi-family company/person convergence over project-family-only BPIN rows; 32 capped company rows now carry the PAE beneficiary-territory signal.
- The new SECOP I legacy/current-risk bridge was materialized locally in run `phase-missing-patterns-secop-i-legacy-current-risk-20260605`, producing 549 hits and 13,949 evidence rows; after guarantee-policy-reuse support refresh the current table/full run emits 556 hits.
- The new SECOP I legacy representative/current-risk bridge was materialized locally in run `phase-missing-patterns-secop-i-representative-current-risk-20260606`, producing 13 hits and 338 evidence rows; after shared-representative same-buyer support refresh the current table/full run emits 14 hits.
- The refreshed SECOP interadministrative executor-network detector was materialized locally in run `phase-missing-patterns-interadmin-network-fiscal-20260605`, producing 698 hits and 10,357 evidence rows; after guarantee-policy-reuse support refresh the current table/full run emits 702 hits.
- The new contract modification-ladder detector was materialized locally in run `phase-missing-patterns-modification-ladder-20260605`, producing 320 hits and 2,767 evidence rows. After refreshing bridge queues, 126 cross-signal rows, 161 interadministrative rows, 48 SECOP I legacy/current rows, and 5 declaration-company bridge rows carry modification-ladder support.
- The new related-bidders same-process detector was materialized locally in run `phase-missing-patterns-related-bidders-20260605`, producing 16 hits and 226 evidence rows. After refreshing bridge queues, 3 cross-signal rows, 1 interadministrative row, and 2 SECOP I legacy/current rows carry related-bidders support.
- The new shared-representative same-buyer detector was materialized locally in run `phase-missing-patterns-shared-representative-same-buyer-20260606`, producing 166 hits and 1,706 evidence rows. After refreshing bridge queues, 26 cross-signal rows, 10 SECOP I legacy/current supplier rows, and 4 SECOP I legal-representative/current-risk rows carry shared-representative same-buyer support.
- The full integration run `phase-missing-patterns-all-signals-shared-representative-20260606` now materializes all 49 supported local signal feature tables, producing 214,095 hits and 581,550 evidence rows.
- `docs/datasets/catalog.report.md` probed 311 datasets and found 148 with schema-level join keys.
- `etl/datasets/` has 149 dataset specs; 36 source IDs currently have local raw lake coverage.
- Several configured signals are intentionally context/optional-only and should not be treated as high-confidence public findings until the missing source is actually loaded.

## Missing patterns

### 1. Same-buyer contract-role-to-supplier overlap

High-confidence pattern: a person/document appears as a SECOP II supervisor, spending orderer, or payment orderer for a buyer and also appears as a supplier to that same buyer.

Why this is missing: current overlap uses SIGEP sensitive positions and RUES officers, but SECOP II contracts already contain role documents (`supervisor_doc_number`, `spending_orderer_doc_number`, `payment_orderer_doc_number`). Those roles are closer to the contract than SIGEP.

Local evidence:

| role field | same-buyer role/supplier pairs | role contracts | supplier contracts | supplier value |
|---|---:|---:|---:|---:|
| `supervisor_doc_number` | 12,558 | 622,920 | 41,689 | COP 9.21T |
| `spending_orderer_doc_number` | 2,033 | 385,681 | 6,386 | COP 224.15B |
| `payment_orderer_doc_number` | 1,461 | 44,328 | 4,490 | COP 131.68B |

False-positive controls:

- Require person-document shape, excluding institutional NITs and buyer NITs. Sample rows show entity NITs can leak into role-document fields.
- Require same buyer, same administrative period, or role date before/near supplier award.
- Prefer reviewer-only until identity normalization is hardened.

Hidden connections:

- Join to `8tz7-h3eu` asset declarations and `gbry-rnq4` conflict disclosures to distinguish declared contractor status from undeclared private interests.
- Join to `procurement_payment_plan_anomalies`, `procurement_contract_suspensions`, and `procurement_large_modifications` to identify roles supervising contracts that later change, suspend, or pay unusually.

Case lead: Centros Poblados shows why supervisor/interventor verification matters. Procuraduria sanctioned officials and contractors after irregularities in precontractual and execution stages; the same bulletin points to false guarantees and omitted validation before an advance payment above COP 70B. Source: https://www.procuraduria.gov.co/Pages/procuraduria-sanciono-exfuncionarios-mintic-y-union-temporal-centros-poblados-2020.aspx

Implementation status: P0 reviewer-only detector is now wired in ETL/registry and materialized locally. Aggregate validation: 9,219 distinct person documents, 932 buyers, and COP 8.97T rolled-up same-buyer supplier value. Role-field rows: 1,351 spending-orderer high, 1,016 payment-orderer high, 275 supervisor high, and 8,669 supervisor medium.

### 2. Declaration-to-supplier chronology and private-interest escalation

High-confidence pattern: the same person/document appears in public asset/conflict declarations and as a procurement supplier, especially where declaration fields indicate contractor status, private economic activity, corporate participation, relatives, donations, or direct interests.

Why this was missing: there was a materialized conflict-disclosure overlap with 65 rows, but it was narrow. The loaded declaration sources supported a much broader exact-document chronology pattern; that first-pass reviewer queue is now materialized.

Local evidence:

- Asset declarations to procurement supplier overlap: 194,225 distinct documents, 1,295,346 award rows, COP 270.52T aggregate award value.
- Conflict declarations to procurement supplier overlap: same order of magnitude because the disclosure datasets share declarant identity coverage.
- SIGEP sensitive-position to supplier overlap: 7,929 distinct documents, 29,532 award rows, COP 1.28T aggregate award value.
- Implemented first pass: 25,000 capped high-severity reviewer rows across 24,589 distinct person documents and 209 buyers, COP 2.64T pair-level contract value. The cap is currently dominated by same-entity/same-buyer chronology rows; 24,640 rows include a contractor-declaration flag, 2,735 include asset private/corporate/board interest signals, 2,149 include conflict-interest flags, and 196 include sensitive-position context.

False-positive controls:

- A declared contractor is not automatically corrupt. Rank only when the person contracts with the same entity, holds a sensitive position, omits or contradicts declared interests, or contracts during a relevant office window.
- Public output should stay aggregated or reviewer-only for people unless exact public identity policy permits exposure.

Hidden connections:

- Combine with same-buyer SECOP role overlap from pattern 1.
- Combine with campaign donor ineligibility from pattern 3 when the declarant is also a donor.
- Combine with RUES officer/company clusters when the person appears through a company rather than as a natural-person supplier.

Case lead: the Vichada case shows the legal importance of donor/contractor conflicts: Procuraduria confirmed a sanction after a governor contracted with a campaign contributor who exceeded the legal contribution threshold. Source: https://www.procuraduria.gov.co/Pages/procuraduria-confirmo-destitucion-inhabilidad-ocho-anos-gobernador-Vichada-irregularidades-contrato.aspx

Implementation status: P0 reviewer-only detector is now wired in ETL/registry and materialized locally. The RUES legal-representative/company bridge is implemented as the adjacent detector below for cases where the declarant supplies through a company rather than as a natural person.

### 2A. Declaration-to-company bridge via RUES legal representative

High-confidence pattern: a public declarant, conflict declarant, or SIGEP sensitive-position person is the RUES legal representative of a company that already appears in current exact-NIT compound-risk evidence.

Why this was missing: the direct declaration chronology queue joined declarant documents to natural-person supplier exposure. It did not cover the common hidden route where the public person is connected to procurement through a company, foundation, mixed entity, E.S.P., fund, or other legal entity registered in RUES.

Implemented first pass:

- `public_declaration_company_bridge_current_risk_review_only` had 56 reviewer-only exact company rows across 55 representative documents in the selected bridge run; after guarantee-policy-reuse support refresh the current table/full run emits 58 rows.
- Severity split: 34 critical and 22 high.
- Disclosure context: 47 rows have asset-disclosure evidence, 47 have conflict-disclosure evidence, and 22 have SIGEP sensitive-position evidence. Thirty-five rows have contractor-declaration context, 17 have asset private/corporate/board-interest context, and 9 have explicit conflict flags.
- Current risk context: 5 rows have five current signal families, 25 have four, and 26 have three. Support counts include 56 procurement-competition rows, 52 execution-failure rows, 21 corporate-capacity rows, 22 conflict-interest rows, 7 service-delivery rows, and 6 sanctions rows.

False-positive controls:

- Exact person document and exact company NIT only.
- Require current compound-risk evidence on the company; do not emit on the RUES bridge alone.
- Sensitive-position-only rows require at least four current signal families.
- Keep reviewer-only. A RUES representative bridge does not prove beneficial ownership, control, undeclared interest, conflict of interest, legal disability, contract irregularity, nonperformance, or corrupt intent.

Hidden connections:

- Declaration/company bridge -> current execution failure, procurement competition, RUES capacity/status, sanctions, or service-delivery queues.
- RUES legal representative -> SIRI antecedent or same-buyer role/supplier queues when the same person document appears in those sources.
- Company bridge -> SECOP I legacy/current-risk bridge when the company also has material historical exposure.

Implementation status: reviewer-only detector `public_declaration_company_bridge_current_risk_review_only` is now wired in ETL/registry and materialized locally. Remaining extensions: reviewer sampling, representative-role date validation, beneficial-ownership inputs, and official-case overlays before any public use.

### 3. Campaign donor legal-ineligibility, not just donor overlap

High-confidence pattern: a campaign donor later receives contracts from the same administrative level and jurisdiction where the candidate was elected, and the donation exceeds the statutory threshold or configured review threshold.

Why this was missing: `cuentas_claras_donor_supplier_overlap` found donor/supplier overlap, but did not model candidate office level, jurisdiction, elected-period timing, or the missing legal inputs needed before classifying ineligibility.

Local evidence:

- `cuentas_claras_donor_supplier_overlap` has 533 materialized rows.
- The Cuentas Claras dataset has candidate identifiers, donor identifiers, municipality/department, party/coalition, voucher date, and amount.
- SECOP has buyer NIT, buyer territory, contract date, modality, and supplier identity.
- Implemented first pass: 322 same-jurisdiction 2019 local-campaign donor-contract review rows, all explicitly marked `missing_campaign_cap` and `missing_election_result`. Aggregate validation: 190 donor/supplier documents, 120 campaigns, 142 buyers, COP 2.919T term contract value, and COP 3.984T campaign-income value. Split: 164 mayor/municipality rows and 158 governor/department rows.

Detector sketch:

1. Normalize donor document/NIT and supplier document/NIT.
2. Resolve candidate office level and elected jurisdiction when available.
3. Compare donation value to the 2 percent legal threshold where campaign-spend cap data is available; otherwise route as "threshold data missing".
4. Flag contracts signed during the elected term with entities in that same administrative level/jurisdiction.
5. Escalate if the contract modality is direct, low competition, emergency, or repeated awards.

Case lead: Procuraduria's Vichada decision explicitly ties the risk to campaign financing, ineligibility, direct contracting, and harm to equal participation. Source: https://www.procuraduria.gov.co/Pages/procuraduria-confirmo-destitucion-inhabilidad-ocho-anos-gobernador-Vichada-irregularidades-contrato.aspx

Implementation status: reviewer-only detector is now wired in ETL/registry and materialized locally. Remaining extension: load election-result and campaign-spending-cap inputs so the queue can distinguish legal ineligibility from threshold-missing review candidates.

### 4. Guarantees, advance payments, and execution failure chain

High-confidence pattern: contracts with high advance payment, weak/missing/late guarantee evidence, low execution or delivery evidence, and later sanctions, fiscal findings, suspensions, or modifications.

Why this was partly missing: the catalog had `gjp9-cutm` SECOP II guarantees, but its YAML had no column map and it was not source-referenced or materialized. The reviewer queue now uses loaded SECOP contract financials, guarantees, execution, suspensions, modifications, and SECOP sanctions while keeping guarantee validity and legal breach as reviewer-only questions.

Relevant datasets:

- `gjp9-cutm` SECOP II Garantias: 6,705,690 loaded policy rows with contract ID, insurer, policy number, creation/send/end dates, status, type/subtype, and value.
- `jbjy-vk9h` SECOP II contracts: `advance_payment_value`, `enables_advance_payment`, `invoiced_value`, `paid_value`, `pending_execution_value`.
- `ibyt-yi2f` invoices and `mfmm-jqmq` execution.
- `u99c-7mfm` suspensions, `u8cx-r425` modifications, `it5q-hg94` sanctions, PACO sanctions.

Implemented evidence:

- `procurement_guarantee_advance_execution_chain` has 257 reviewer-only rows, 243 exact suppliers, and 101 buyers.
- Aggregate contract value is COP 1.203T, with COP 251.30B in advance-payment value and COP 1.066T in pending-execution value.
- Severity split: 21 critical and 236 high.
- Chain flags: 257 ended-pending-execution rows, 257 suspension rows, 187 high-advance rows, 49 large-modification rows, 19 SECOP-sanction rows, and 2 execution-delay rows.
- Guarantee evidence split after loading `gjp9-cutm`: 235 rows have guarantee records, 194 have accepted-policy evidence, 22 have no SECOP guarantee row, 41 have guarantee rows without an accepted policy, 25 have first policy sent after contract start, and 139 have at least one guarantee issue flag. The v2 materialization produced 1,505 evidence rows, including 704 `secop_guarantees` evidence rows.

Case leads:

- Centros Poblados: false guarantees and advance-payment validation failures were central to the official disciplinary narrative. Source: https://www.procuraduria.gov.co/Pages/procuraduria-sanciono-exfuncionarios-mintic-y-union-temporal-centros-poblados-2020.aspx
- UNGRD La Guajira: official investigation references water tanks/carro-tanques, delivery failures, missing mandatory policies, and possible noncompliance. Source: https://www.procuraduria.gov.co/Pages/procuraduria-abrio-nueva-investigacion-olmedo-lopez-exdirector-ungrd-posibles-irregularidades-compra-tanques.aspx

Implementation status: reviewer-only detector is wired in ETL/registry and materialized locally with SECOP guarantee evidence. Remaining P0 extension is invoice/payment timing, stronger guarantee-validity semantics, and reviewer calibration before any public claim about guarantee defects.

### 4A. Guarantee policy reuse across suppliers and buyers

High-confidence pattern: the same normalized SECOP guarantee insurer and policy number appears on accepted or expired guarantee records for different suppliers and different buyers, with high aggregate contract exposure.

Why this was missing: the guarantee/advance/execution-chain detector uses guarantee status/date/value evidence inside a contract-execution chain, but it does not look for hidden guarantee-identity reuse across unrelated contracts. Colombia cases around false or weak guarantee validation make this a distinct review surface, while still requiring insurer confirmation before any statement about policy validity.

Implemented hard gates:

- Exact SECOP contract ID join from `gjp9-cutm` guarantees to SECOP II award facts.
- Normalize insurer and policy number; require a policy-like identifier with at least 8 digits and remove text placeholders such as `No Definido`, `cumplimiento`, `poliza`, `seguro`, `garantia`, `anexo`, insurer names, and all-zero/all-one/all-nine values.
- Require small clusters of 2 to 5 contracts, at least 2 distinct suppliers, at least 2 distinct buyers, all cluster rows accepted or expired, and at least COP 1B aggregate contract value.
- Emit one reviewer-only row per contract/supplier in the cluster, with current contract evidence, guarantee evidence, and other cluster member evidence.

Implemented evidence:

- `procurement_guarantee_policy_reuse_review_only` has 723 reviewer-only rows across 345 insurer-policy clusters, 660 suppliers, 317 buyers, and 701 distinct contracts.
- Severity split: 62 critical rows and 661 high rows.
- Cluster-size split: 317 two-contract clusters, 23 three-contract clusters, and 5 four-contract clusters.
- Queued row contract value is COP 2.248T. All rows have different-supplier, different-buyer, small-cluster, high-value, and accepted/expired guarantee flags; 42 rows also have very-high-value cluster flags.
- Selected materialization run `phase-missing-patterns-guarantee-policy-reuse-20260605` produced 723 hits and 3,110 evidence rows. After refreshing bridge queues, 43 cross-signal rows, 64 interadministrative rows, 11 SECOP I legacy/current rows, and 3 declaration-company bridge rows carry guarantee-policy-reuse support.

False-positive controls:

- This detector does not prove that a reused policy is false, invalid, insurer-denied, unauthorized, legally defective, or corrupt.
- Reviewers must validate the policy file, insurer confirmation, endorsements, contract file, and any legitimate umbrella/master-policy explanation before escalation.
- Very large or text-like clusters are intentionally excluded to avoid broad placeholder or master-policy noise.

### 5. Corporate capacity, RUES status, and post-status contracting

High-confidence pattern: supplier wins material public contracts despite RUES status/capacity red flags: cancelled registration, very recent incorporation, stale renewal, mismatch between CIIU and contract object, many branches/shell-like registrations, or contract value far beyond observed corporate capacity.

Why this was missing: `procurement_related_companies_shared_officer` and `procurement_cross_source_identity_inconsistency` used RUES/company sources, but did not model capacity, status transitions, renewal recency, or registration-age risk. A first-pass reviewer queue now covers the strongest loaded RUES/SECOP joins.

Local evidence:

- 6,434 RUES companies overlap procurement suppliers in the current local lake.
- Among those, 240 have `CANCELADA` status and 139 show cancellation due to domicile transfer.
- A rough status-date query found 377 companies with contracts after a parsed cancellation date, but some active rows also carry cancellation dates. This requires better status-event normalization before public use.
- Implemented first pass: 1,044 exact-NIT supplier rows, all reviewer-only, with COP 37.504T total procurement exposure. Severity split: 155 high and 889 medium. Flag split: 507 recent-registration large-award rows, 434 multi-matricula supplier rows, 73 post-inactive-contract rows, and 35 stale-renewal high-value rows. Five rows have multiple RUES/capacity flags.

False-positive controls:

- Treat RUES cancellation dates carefully; active records can still carry historical cancellation-like values.
- Require current status normalization, chamber, matricula category, and if possible branch history.
- Use CIIU/object mismatch as a reviewer score, not a standalone public finding.
- The materialized queue requires exact NIT supplier matching and excludes common placeholder NITs. Post-inactive contracting requires no active RUES row for the supplier document; if active and inactive records coexist, the row is treated as review context instead of active-cancellation proof.

Case leads:

- Centros Poblados involved concerns about bidder capacity and financial/requisite changes in the pre-award stage. Source: https://www.procuraduria.gov.co/Pages/procuraduria-sanciono-exfuncionarios-mintic-y-union-temporal-centros-poblados-2020.aspx
- OCAD Paz Miranda: Procuraduria investigated whether a mixed fund had the experience and technical/administrative capacity to execute a road project financed with royalties. Source: https://www.procuraduria.gov.co/Pages/investigacion-contra-exmiembros-ocad-paz-por-irregularidades-en-proyecto-financiado-con-regalias-.aspx

Implementation status: reviewer-only detector `rues_supplier_capacity_status_review_only` is now wired in ETL/registry and materialized locally. Remaining extensions: normalize RUES status-event history by chamber/matricula, add CIIU/object mismatch, branch/domicile movement (`nb3d-v3n7`), and financial-capacity evidence when NIIF/Supersoc inputs are mapped.

### 6. OCAD/SGR executor capacity and royalty-project delivery gap

High-confidence pattern: BPIN/SGR projects where an executor or contractor with weak capacity/experience receives large royalty-funded work and the project later shows low physical/financial execution, repeated modifications, sanctions, or contract concentration.

Why this was missing: previous SGR/BPIN signals were project and territory overlaps. They did not rank low physical/financial execution against linked SECOP payments, RUES supplier-capacity flags, or procurement failure chains. A first-pass reviewer queue now covers the strongest loaded joins.

Local evidence:

- `project_regalias_execution_procurement_overlap`: 41 rows.
- `bpin_dnp_vs_pida27_obras_prioritarias`: 796 rows.
- Hidden overlap: 16 BPIN projects are flagged by both priority-work SECOP awards and SGR execution/procurement overlap. Current top overlaps cluster in Meta/Villavicencio.
- Loaded SGR/DNP sources include `qkv4-ek54` expense execution, `mzgh-shtp` SGR projects, `epzv-8ck4` DNP executors, `xikz-44ja` DNP project locations, `iuc2-3r6h` DNP beneficiary locations, and `tmmn-mpqc` DNP beneficiary characterization.
- Implemented first pass: 60 exact-BPIN project rows, 33 executors, 14 populated departments, and 11 sectors. Aggregate project value is COP 2.849T, linked SECOP contract value is COP 2.581T, and SGR payments total COP 1.527T.
- Severity split: 27 critical, 16 high, and 17 medium. Flag split: 60 low SGR execution rows, 30 paid-low-physical rows, 39 high-contracting-low-physical rows, 39 procurement-failure rows, 7 RUES supplier-capacity rows, and 12 OCAD Paz/project-paz rows.
- Implemented DNP extension: `dnp_sgr_beneficiary_delivery_gap_review_only` adds 60 exact-BPIN reviewer rows using the same SGR capacity-gap queue plus DNP executor/location/beneficiary context. Severity split is 31 critical, 23 high, and 6 medium. Aggregate project value is COP 2.849T, linked SECOP contract value is COP 2.581T, and SGR payments total COP 1.527T.
- DNP-context flags: 48 rows have vulnerable-population context, 44 have high demographic-observation context, 39 have a procurement-failure chain, 13 have multi-territory beneficiary context, and 12 are OCAD Paz/project-paz rows. The current cross-signal cap emits 0 direct DNP project rows, so this is a project-family hidden connection between SGR, SECOP, and DNP rather than a company/person cross-signal support claim.
- Sector concentration: 26 transport rows, 9 education rows, and 7 agriculture/rural-development rows.

Case leads:

- OCAD Paz Miranda: official investigation focuses on executor capacity and an 18 percent project-management index. Source: https://www.procuraduria.gov.co/Pages/investigacion-contra-exmiembros-ocad-paz-por-irregularidades-en-proyecto-financiado-con-regalias-.aspx
- OCAD Paz Cauca road contract: official investigation references 100 percent reported advance versus incomplete/faulty works. Source: https://www.procuraduria.gov.co/Pages/investigacion-por-presuntas-irregularidades-contrato-de-ocad-paz-por-mas-de-13-mil-millones.aspx
- Procuraduria also reported 24 disciplinary actions over OCAD Paz projects near COP 250B. Source: https://apps.procuraduria.gov.co/portal/PROCURADURIA-ADELANTA-24-ACTUACIONES-DISCIPLINARIAS-POR-PRESUNTAS-IRREGULARIDADES-EN-PROYECTOS-DEL-OCAD-PAZ-POR-CERCA-DE-_-250-MIL-MILLONES.news

Implementation status: reviewer-only detector `sgr_ocad_executor_capacity_gap` and DNP extension `dnp_sgr_beneficiary_delivery_gap_review_only` are now wired in ETL/registry and materialized locally. The DNP extension does not prove beneficiary harm, non-delivery, incorrect targeting, legal breach, or corrupt intent. Remaining extensions: add financial-capacity evidence for executors and contractors, official case overlays, downstream subcontract evidence, and OCAD Paz/PDET/PNIS priority calibration before any public finding.

### 7. Interadministrative agreements and special-purpose executors

High-confidence pattern: interadministrative agreements, mixed funds, patrimonios autonomos, or special-purpose entities receive projects outside their demonstrated sector capacity, then subcontract/execute through concentrated supplier networks.

Why this was missing: `secop_interadmin_agreements` was promoted in the catalog but had no materialized signal. The active weekly mirror is now loaded and a first-pass reviewer queue treats it as a bridge table to exact-NIT procurement/capacity/execution/sanction evidence.

Relevant datasets:

- `ityv-bxct` and `s484-c9k3` SECOP convenios interadministrativos.
- `dnp_project_executors`, `sgr_projects`, `secop_process_bpin`.
- RUES company registry and branches.
- SECOP contracts, additions, modifications, suspensions.

Hidden connections:

- Interadmin agreement -> BPIN/SGR project -> executor -> subcontract/supplier concentration.
- Mixed fund executor -> no matching CIIU/experience -> priority infrastructure contract.
- Agreement actor -> official bulletin/judicial/procuraduria document overlap when official-case bulletins are ingested.

Implemented first pass:

- `s484-c9k3` loaded 88,522 SECOP interadministrative agreement rows.
- `secop_interadmin_executor_network_review_only` had 698 reviewer-only rows across 252 exact-NIT recipients and 638 agreement IDs in the selected run; after guarantee-policy-reuse support refresh the current table/full run emits 702 rows.
- Aggregate agreement value is COP 12.673T. Severity split: 115 critical and 583 high.
- 374 rows involve special-executor text such as funds, ESEs, universities, associations, mixed entities, or public-service companies; 263 rows now carry health/PAE service-delivery support.
- Dominant family combinations: execution failure + procurement competition (310 rows), execution failure + procurement competition + service delivery (235), procurement competition + sanctions (33), corporate capacity + procurement competition (28), and corporate capacity + execution failure + procurement competition (20).

Case lead: OCAD Paz examples above repeatedly mention Fondo Colombia en Paz, mixed funds, INVIAS, and project execution questions.

Implementation status: reviewer-only detector `secop_interadmin_executor_network_review_only` is now wired in ETL/registry and materialized locally. Remaining extensions: map downstream subcontract paths more explicitly, join to BPIN/SGR executor/location evidence, and add official-case bulletin overlays before any public finding.

### 8. PAE and health-provider service-delivery fraud

High-confidence pattern: education/health service contracts where billed or modified amounts diverge from beneficiary/service evidence, especially with health providers, PAE operators, interventoria/supervision failures, or false service records.

Why this was missing: `health_providers` and direct PAE context were promoted but not loaded. The REPS source is now ingested and the first reviewer-only detector uses exact REPS provider NITs or explicit SECOP health/PAE object text plus independent payment, budget, execution, modification, suspension, guarantee, or competition support. The MEN PAE indicator source is now also loaded and used as municipal beneficiary context for PAE-keyword contracts.

Relevant datasets:

- `c36g-9fc2` health providers by NIT: 76,821 loaded REPS provider-site rows, 57,537 distinct identification numbers, 61,073 provider codes, and 76,821 site codes.
- SECOP contracts and objects for PAE/health terms.
- Invoices, payment plans, execution, suspensions, additions, and sanctions.
- MEN PAE indicators (`epkg-mphw`): 121,379 loaded rows by municipality, zone, school day, population group, and beneficiary count.

Implemented first pass:

- `health_pae_service_delivery_gap_review_only` has 639 reviewer-only rows across 462 entities and 210 buyers, with COP 593.973T in queued contract value.
- Severity split: 380 critical and 259 high.
- Domain evidence: 208 exact REPS provider matches, 510 health-keyword rows, and 78 PAE-keyword rows.
- Domain split: health keyword contracts (374 rows), REPS provider health contracts (132), PAE school feeding (57), REPS provider contracts (55), and health-provider PAE contracts (21).
- Support families: payment/budget + procurement competition (361 rows), execution failure + procurement competition (255), execution failure + payment/budget (14), and all three families (9).
- Implemented MEN PAE extension: `pae_beneficiary_territory_delivery_gap_review_only` adds 64 reviewer-only exact supplier rows for PAE-keyword contracts already in the service-delivery queue and matched to official MEN beneficiary territory context. Severity split is 33 critical and 31 high.
- MEN context split: 18 rows have same-year PAE indicator context and 46 use the latest prior PAE year available for that municipality. PAE years in the current queue are 2020 (3 rows), 2021 (15), and 2022 (46), reflecting the loaded MEN source coverage. Top departments by row count are Cauca (22), Antioquia (11), Casanare (7), Boyaca (5), Atlantico (4), Arauca (3), and Narino (3).
- The PAE extension is a hidden connection between SECOP delivery-risk contracts and MEN municipal beneficiary populations. It does not prove beneficiary mismatch, non-delivery, ration quality failure, overbilling, legal breach, or corrupt intent; it tells reviewers where contract files should be compared against MEN/school/service records.
- This is not proof of false services, overbilling, delivery failure, provider ineligibility, beneficiary mismatch, or corrupt intent. Review still needs service-line REPS status, beneficiary/patient records, audit findings, and contract-file support.

Case leads:

- Arauca PAE: Procuraduria found overcosts and value/term additions tied to deficient prior studies and supervision. Source: https://www.procuraduria.gov.co/Pages/sancionados-cinco-exfuncionarios-gobernacion-arauca-irregularidades-contratacion-pae.aspx
- Cartagena PAE: Procuraduria reported overcosts and detriment in an alimentation contract. Source: https://www.procuraduria.gov.co/Pages/sobrecostos-527-millones-contratacion-pae-procuraduria-suspendio-exalcalde-exdirectora-cobertura-educativa-cartagena.aspx
- Cordoba hemophilia: Fiscalia reported a conviction tied to payments to an IPS for treatments without supporting service evidence, including false medical documentation. Source: https://www.fiscalia.gov.co/colombia/lucha-contra-corrupcion/condenado-exsecretario-de-salud-de-cordoba-por-facilitar-el-pago-de-falsos-tratamientos-a-pacientes-con-hemofilia-y-von-willebrand/

Implementation status: reviewer-only detectors `health_pae_service_delivery_gap_review_only` and `pae_beneficiary_territory_delivery_gap_review_only` are now wired in ETL/registry and materialized locally. Remaining extensions: promote health service-line authorization where available, join direct school/service evidence beyond municipal MEN indicators, join TVEC unit-price comparators for medical/food items, and add official-case bulletin overlays before any public finding.

### 9. Contract modification ladder inspired by Odebrecht

High-confidence pattern: mega-contracts or concession-like contracts with multiple otrosies/modifications that change payment formula, add works, delay execution, or materially increase value, especially when modifications avoid new competitive selection.

Why this was missing: `procurement_large_modifications` existed, but it was mostly value-increase detection. Odebrecht-style conduct is about a sequence of modifications, financial-term changes, new works, delays, and institutional actors.

Relevant datasets:

- `u8cx-r425` contract modifications.
- SECOP contract values, dates, modalities, objects.
- SECOP additions, suspensions, invoices, budget commitments.
- Role-document overlap from pattern 1.

Implemented first pass:

- `procurement_contract_modification_ladder_review_only` has 320 reviewer-only exact-contract rows across 307 suppliers and 129 buyers.
- Severity split: 264 critical and 56 high.
- Aggregate queued contract value is COP 1.489T, with COP 104.248T in source modification value and 130,377 total extended days. Source values can be inflated by versioning semantics, so the queue remains reviewer-only.
- Flag split: 320 material-value rows, 91 major-delay rows, 69 payment/financial-term rows, 229 scope/change rows, and 243 direct/special/obra/concession-style modality-context rows.
- Support split: 309 rows already overlap `procurement_large_modifications`, 132 overlap single-bidder support, 48 budget-chain support, 47 guarantee-chain support, 39 suspension support, 14 invoice support, and 1 execution-delay support.
- Hidden-connection impact after refresh: 126 cross-signal rows, 161 interadministrative rows, 48 SECOP I legacy/current rows, and 5 declaration-company bridge rows carry modification-ladder support.

Case lead: Fiscalia's Odebrecht case page repeatedly identifies Ruta del Sol II other contractual additions/modifications, payment-condition changes, and favoring of the concessionaire. Source: https://www.fiscalia.gov.co/caso-odebrecht/

Implementation status: reviewer-only detector is wired in ETL/registry and materialized locally. It does not prove illegal modification, unlawful avoidance of competition, fiscal harm, delivery failure, or corrupt intent without contract-file and legal review.

### 9A. Related bidders in the same SECOP process

High-confidence pattern: a SECOP II process where the awarded supplier and a competing offerer are distinct exact-NIT companies but share the same exact RUES legal representative/officer.

Implemented first pass:

- `procurement_related_bidders_same_process_review_only` has 16 reviewer-only exact-process rows across 7 winning suppliers, 7 related offerers, 16 processes, and 15 buyers.
- Severity split: 13 critical and 3 high.
- Aggregate queued contract value is COP 18.93B. Bidder-count distribution: 11 rows have 2-4 valid offerers, 2 rows have 8, and 3 rows are broader processes retained because the pair repeats or value is very high.
- Hard gates require exact SECOP process id, exact winner NIT, exact competing-offerer NIT, distinct companies, exact shared RUES representative document, at least two valid offerers, contract value >= COP 500M, and either bounded competition, repeated pair, or COP 1B+ value.
- Hidden-connection impact after refresh: 3 cross-signal rows, 1 interadministrative row, and 2 SECOP I legacy/current rows carry related-bidders support.

False-positive controls:

- Shared representation can be lawful corporate-group or insurance/consortium structure. The queue is reviewer-only and does not prove collusion, beneficial ownership, control, simulated competition, bid suppression, legal ineligibility, breach, or corrupt intent.
- Broad tenders are retained only when value is very high or the same related pair repeats.

### 9B. Shared representative suppliers awarded by the same buyer in the same year

High-confidence pattern: the same exact RUES legal representative is connected
to multiple distinct supplier companies that receive awards from the same
public buyer in the same year, with material value and either direct/exception
contracting, high contract count, or COP 10B+ cluster exposure.

Why this was missing: `procurement_related_bidders_same_process_review_only`
catches shared representation inside one SECOP process, and
`procurement_related_companies_shared_officer` catches broad shared-officer
supplier clusters. Neither asks whether one buyer repeatedly awarded multiple
companies tied to the same representative across the same annual procurement
cycle.

Implemented first pass:

- `procurement_shared_representative_same_buyer_cluster_review_only` has 166
  reviewer-only supplier rows across 77 representative/buyer/year clusters, 44
  representatives, 35 buyers, and 95 exact-NIT suppliers.
- Cluster exposure totals COP 2.201T at supplier-row level. Clusters have two
  or three supplier companies, three to 23 contracts, and at least COP 5.004B
  total contract value.
- Severity split: 15 critical and 151 high.
- All 166 rows satisfy exact RUES representative bridge, same buyer/year
  multi-supplier cluster, high-value cluster, direct/high-value/high-volume
  pressure, and bounded representative-company flags.
- Selected materialization run
  `phase-missing-patterns-shared-representative-same-buyer-20260606` produced
  166 hits and 1,706 evidence rows.
- Hidden-connection impact after refresh: 26 cross-signal rows, 10 SECOP I
  legacy/current supplier rows, and 4 SECOP I legal-representative/current-risk
  rows carry shared-representative same-buyer support.

False-positive controls:

- Keep reviewer-only. A shared legal representative can reflect a lawful
  business group, accountant/lawyer representation, temporary RUES update,
  consortium ecosystem, or buyer market structure.
- Require exact NIT suppliers and exact RUES representative documents.
- Bound representative company count to avoid generic mass-representative
  false positives.
- Require same buyer and same year, two to six suppliers, at least three
  contracts, COP 5B+ cluster value, and direct/exception or high-volume support.

Hidden connections:

- Same-buyer shared-representative cluster -> cross-signal compound risk when
  the companies also have execution, corporate-capacity, service-delivery,
  sanction, campaign, or declaration support.
- Same representative -> SECOP I legal-representative bridge to detect
  historical procurement exposure migrating into current same-buyer clusters.
- Same buyer/year -> budget, invoice, payment-plan, guarantee, modification, or
  suspension queues to separate lawful business-group concentration from
  execution or money-flow red flags.

### 10. Budget-chain leakage: CDP, commitments, rubros, invoices, and contract value

High-confidence pattern: contract value, CDP, commitment, invoice, and payment fields diverge materially, or money is committed/paid with missing budget-chain support.

Why this was missing: `secop_budget_commitments`, `secop_budget_items`, and `secop_cdp_requests` were cataloged/configured but not loaded locally. They are now mapped, loaded, and joined in a first-pass reviewer queue. SECOP II invoices and payment plans are now loaded and joined in adjacent reviewer queues for invoice/payment timing, CUFE, supplier-document, and budget-commitment references.

Relevant datasets:

- `a86w-fh92` CDP requests: 9,572,873 loaded rows.
- `skc9-met7` commitments: 5,866,580 loaded rows.
- `cwhv-7fnp` budget items/rubros: 5,891,594 loaded rows.
- `ibyt-yi2f` invoices: 21,002,798 loaded rows with exact `id_contrato`, invoice ID/status/value/date, estimated-payment date, and confirmation fields.
- `uymx-8p3j` payment plans: 17,480,966 loaded rows with exact `id_del_contrato`, payment ID, CUFE, real payment date, invoice dates, commitment reference, provider document, and supervisor document.
- SECOP contracts and modifications.

Hidden connections:

- Advance payment + missing/late guarantee + budget commitment mismatch.
- Contract additions + commitment expansions + same supplier concentration.
- BPIN/SGR project + budget-chain divergence.

Implemented evidence:

- `procurement_budget_chain_reconciliation_review_only` has 1,000 capped reviewer-only rows across 827 exact suppliers and 368 buyers.
- Aggregate contract value in the capped queue is COP 1,364.83T. The value-ranked subset contains very large source values, so the strongest review slice is the exact-contract subset that also has guarantee/advance/execution-chain evidence.
- Severity split: 982 critical and 18 high.
- 257 rows also carry `procurement_guarantee_advance_execution_chain` evidence.
- Budget flags: 923 CDP-only-weak-SIIF rows, 287 commitment-only-failed rows, 44 missing-CDP rows, 687 missing-commitment rows, 687 missing-rubro rows, 801 CDP-under-contract rows, 826 commitment-under-contract rows, 141 commitment-over-contract rows, and 310 zero/undefined-rubro rows.
- Materialized evidence split: 1,171 CDP refs, 1,000 SECOP contract refs, 678 budget-item refs, 678 commitment refs, and 257 guarantee-chain feature refs.
- `procurement_invoice_budget_reconciliation_review_only` adds 376 exact-contract reviewer rows across 343 exact suppliers and 156 buyers after decimal-safe invoice value parsing and contract-award de-duplication.
- Invoice queue severity split: 149 critical and 227 high. Main anomaly split: 233 invoice-value-over-contract rows, 45 confirmed-invoice-over-contract rows, 72 post-contract-end invoice rows, and 26 rows where SECOP invoice rows contradict a zero paid/invoiced contract summary.
- Invoice queue context/evidence: 99 rows overlap the budget-chain detector, 92 overlap guarantee/advance/execution-chain, and the materialized run produced 1,603 evidence rows: 1,036 invoice refs, 376 SECOP contract refs, 99 budget-chain refs, and 92 guarantee-chain refs.
- `procurement_payment_plan_reconciliation_review_only` adds 1,000 capped critical exact-contract reviewer rows across 311 exact suppliers and 210 buyers. Main anomaly split: 976 actual-payment-over-contract rows and 24 duplicate-CUFE paid-across-contract rows.
- Payment-plan flag split: 993 actual-paid-over-contract rows, 650 post-contract-end real-payment rows, 243 payment-before-invoice-sequence rows, 7 supplier-document mismatch rows, and 24 duplicate-CUFE rows. Context split: 28 invoice-chain overlaps, 2 budget-chain overlaps, and 1 guarantee-chain overlap. The materialized run produced 3,934 evidence rows: 2,903 payment-plan refs, 1,000 SECOP contract refs, 28 invoice-chain refs, 2 budget-chain refs, and 1 guarantee-chain ref.

Implementation status: the reviewer-only budget-chain, invoice-budget, and payment-plan reconciliation detectors are wired in ETL/registry and materialized locally. Remaining work is reviewer sampling, source-value/CUFE/status calibration, and SIIF/treasury/contract-file validation before any public claim about budget or payment irregularity.

### 11. TVEC item-price dispersion and buyer overpayment

High-confidence pattern: a buyer pays materially above the market-normal price distribution for the same TVEC item/unit, especially when the supplier also shows high buyer capture or SECOP concentration.

Why this was missing: `tvec_multi_entity_capture` is materialized, but it is supplier-level. It asks "who sells broadly across the state?" rather than "what item was bought at an anomalous price?" The first-pass reviewer queue is now materialized.

Local evidence:

- `3hdv-smhz` has 1,400,581 loaded TVEC item purchase rows with `order_id`, `item_name`, `unit_price`, `quantity`, `unit`, `line_total`, buyer NIT, and supplier NIT.
- A rough screen over loaded TVEC rows found 1,001,457 valid price rows, 2,423 comparable item/unit groups with at least 20 rows, 3 suppliers, and 5 buyers, and 861 groups where p90 unit price was at least 2x the median.
- Under that rough screen, high-price lines covered 12,188 line items, 7,941 orders, 581 suppliers, and COP 3.53T in line value.
- Implemented first pass: 4,601 reviewer-only order/item/supplier hits across 432 normalized item keys, 3,297 orders, 412 suppliers, and 413 buyers; flagged line value is COP 1.74T. Severity split: 4,411 high and 190 medium. The queue uses item name, unit, quantity band, and year; requires at least 20 comparable lines, 3 suppliers, 5 buyers, p90 >= 2x median, and observed unit price >= p95 and >= 2x median.

False-positive controls:

- Start reviewer-only. TVEC item names include service bundles and budget placeholders, not only homogeneous goods.
- Require exact catalog item code or normalized item plus unit, quantity band, agreement/category, and date window.
- Exclude labels such as generic `presupuesto` unless there is a stronger catalog key.
- Compare against the same framework agreement or category when possible, not against all TVEC rows.
- Escalate only when high unit price combines with same-buyer concentration, emergency/direct contracting context, short bidding windows, or repeat awards.

Hidden connections:

- Join high-price TVEC lines to `tvec_multi_entity_capture` supplier rows.
- Join buyer/supplier pairs to SECOP repeat awards, sanctions, suspensions, and modifications.
- Use PAE, health, emergency, and defense item keywords as category filters when domain-specific evidence exists.

Case lead: official Procuraduria cases repeatedly focus on overpricing relative to market studies and cotizaciones, including biosecurity inputs allegedly 104 percent above market in Encino, Santander, and first-aid items in Suba allegedly above quoted prices. Sources: https://www.procuraduria.gov.co/Pages/presunto-sobrecosto-compra-insumos-bioseguridad-procuraduria-formulo-cargos-exalcalde-encino-santander.aspx and https://www.procuraduria.gov.co/Pages/presuntos-sobrecostos-insumos-medicos-procuraduria-indaga-funcionarios-alcaldia-suba.aspx

Implementation status: P0 reviewer-only detector is now wired in ETL/registry and materialized locally. Remaining extensions: join supplier/buyer pairs to existing SECOP concentration, sanctions, suspension, and modification signals; add stronger catalog/framework-agreement keys when available.

### 12. SIRI antecedent and ineligibility chronology

High-confidence pattern: a supplier, representative, supervisor, orderer, or public servant has official disciplinary antecedents or ineligibility records and still participates in procurement during a legally relevant period.

Why this was partly missing: `procurement_sanctioned_supplier_awarded` already uses PACO/fiscal/SECOP sanction-style overlaps, and `procurement_secop_sanction_later_awards_review_only` now covers loaded SECOP II sanction-contract records followed by later exact supplier awards. The larger SIRI antecedent/ineligibility model was missing because `iaeu-rcn6` SIRI antecedents were cataloged but not locally mapped; a first-pass reviewer queue now maps SIRI document/date/duration fields and joins exact person documents to supplier, contract-role, and RUES legal-representative procurement exposure. Direct Contraloria fiscal findings/responsibility sources were also cataloged but not locally loaded; they are now mapped and covered by a separate exact-NIT fiscal chronology queue.

Relevant datasets:

- `iaeu-rcn6` SIRI antecedents: 43,491 loaded rows with person/company document, SIRI number, process reference, legal-effects date, sanction text, and duration fields.
- `jbjy-vk9h` SECOP II contracts: supplier, legal representative, supervisor, spending orderer, payment orderer.
- `5u9e-g5w9` sensitive positions and `8tz7-h3eu`/`gbry-rnq4` declarations.
- Existing PACO and SECOP sanction sources.
- `8qxx-ubmq` Hallazgos Fiscales: 73 loaded rows, 20 distinct subject documents.
- `jr8e-e8tu` Responsabilidad Fiscal: 60 loaded rows, 27 distinct subject documents.
- Implemented loaded SECOP subset: 127 SECOP sanction-contract chronology rows, 103 exact suppliers, 120 sanctioned contracts, 47 sanctioning buyers, 1,928 later contract references, COP 817.28B later-award value, and COP 402.81B same-buyer later-award value. This is reviewer-only and does not prove final sanction status or legal ineligibility.
- Implemented fiscal first pass: 80 reviewer-only rows across 32 entities and 80 fiscal records, with 5,699 later contract references, COP 26.154T later procurement value, and COP 342.17B fiscal amount. Severity split: 47 critical, 27 high, and 6 medium. Source split: 47 fiscal-responsibility critical rows with COP 25.663T later value and COP 335.37B fiscal amount; 27 fiscal-finding high rows with COP 489.48B later value and COP 6.756B fiscal amount; 6 fiscal-finding medium rows with COP 2.272B later value and COP 39.1M fiscal amount. This queue does not prove current legal disability, final fiscal liability for finding rows, contract illegality, public-entity role misuse, or corrupt intent.
- Implemented SIRI first pass: 1,120 reviewer-only rows across 103 person documents, 768 contracts, and COP 47.59B procurement value. Severity/status split: 903 critical active-ineligibility-inferred rows, 162 high active-ineligibility-inferred rows, and 55 medium duration-missing-post-effect rows. Exposure split: 475 supervisor rows, 362 spending-orderer rows, 166 direct-supplier rows, and 117 RUES legal-representative supplier rows.

False-positive controls:

- Distinguish antecedent existence from active legal disability. The detector must parse sanction type, effective dates, and whether the record creates inhabilidad/incompatibilidad.
- Treat person-document hits as reviewer-only until exact identity policy and legal-status parsing are explicit.
- Rank higher when the same document is also a supplier, legal representative, contract role, campaign donor, or RUES officer.

Hidden connections:

- SIRI antecedent -> public office role -> contract-role-to-supplier overlap.
- SIRI antecedent -> company representative/officer -> supplier award.
- SIRI antecedent -> campaign donor legal-ineligibility queue.
- Fiscal finding/responsibility -> later exact supplier awards -> cross-signal compound or interadministrative recipient evidence.

Implementation status: SECOP sanction later-awards, fiscal procurement chronology, and SIRI antecedent/procurement chronology subsets are now wired in ETL/registry and materialized locally. Remaining P0 work is stronger legal-status/finality/current-disability parsing, donor/declaration joins, and reviewer calibration before any public finding.

### 13. SECOP I legacy supplier to current compound-risk bridge

High-confidence pattern: an exact-NIT supplier has material historical SECOP I exposure and now appears in the current compound-risk queue across independent signal families.

Why this was missing: `secop_i_historical_processes` was loaded and promoted, but the shipped detectors were dominated by SECOP II/current-period joins. That left a time-spanning blind spot: suppliers with large pre-SECOP-II exposure, additions, direct/special-regime modalities, or broad buyer reach were not being connected to present-day sanctions, execution, competition, RUES, declaration, campaign, service-delivery, or other current-risk evidence.

Relevant datasets:

- `qddk-cgux` SECOP I historical processes: 6,123,394 loaded rows with contractor NIT, buyer/entity fields, contract/process values, additions, modality/regime, signing year/date, BPIN/T302/postconflict context where present, and SECOP I process URL.
- `cross_signal_compound_risk_review_only`: current exact identity convergence across procurement competition, sanctions, execution failure, corporate capacity, service delivery, campaign finance, conflict-interest, and project/royalties families.

Implemented first pass:

- `secop_i_legacy_supplier_current_risk_review_only` had 549 reviewer-only exact-NIT supplier rows in the selected run, all joined to current compound-risk entities; after guarantee-policy-reuse support refresh the current table/full run emits 556 rows.
- Severity split: 154 critical and 395 high.
- Historical exposure totals: COP 77.083T SECOP I contract value, COP 10.912T additions, and 15,960 SECOP I legacy contracts.
- Historical context: 1,593 material-addition legacy rows and 10,754 direct/exception-modality legacy rows.
- Current compound-risk context: 312 rows have critical base cross-signal severity and 237 have high base severity; 7 rows have five current signal families, 81 have four, and 461 have three.
- Current family support counts: 545 procurement-competition rows, 478 execution-failure rows, 160 service-delivery rows, 115 corporate-capacity rows, 61 sanctions rows, 32 campaign-finance rows, and 16 conflict-interest rows.

False-positive controls:

- Exact company NIT only.
- Require at least COP 1B SECOP I legacy exposure and at least two historical contracts.
- Require current exact-NIT compound-risk evidence rather than legacy exposure alone.
- Keep reviewer-only. The bridge does not prove continuity of management, legal disability, legacy irregularity, current irregularity, contract breach, nonperformance, or corrupt intent.

Hidden connections:

- SECOP I direct/special-regime or addition-heavy supplier -> current execution failure, sanctions, or service-delivery queue.
- Legacy broad-buyer exposure -> current supplier concentration, RUES capacity/status, or interadministrative recipient network.
- Legacy BPIN/T302/postconflict context where present -> current SGR/DNP or vulnerable-service review priorities.

Case lead: Odebrecht-style cases show why long-running procurement histories, additions/modifications, and current actor networks should be reviewed together, but the detector remains a triage bridge and not a case-match claim. See the official case anchor in pattern 9.

Implementation status: reviewer-only detector `secop_i_legacy_supplier_current_risk_review_only` is now wired in ETL/registry and materialized locally. Remaining extensions: add SECOP I additions/modification companion sources where available, stronger contractor corporate-history continuity, official-case bulletin overlays, and reviewer sampling before any public use.

### 13A. SECOP I legal representative to current compound-risk company bridge

High-confidence pattern: a person appears as legal representative in material SECOP I historical procurement and is currently the RUES legal representative of a different exact-NIT company already in current compound-risk evidence.

Why this was missing: the exact-NIT SECOP I bridge only catches the same supplier across time. It does not catch representative continuity, management migration, or person-mediated reuse of historical procurement exposure through a different current company.

Implemented hard gates:

- Exact SECOP I legal-representative document and exact current RUES legal-representative document.
- Current company must already be in `cross_signal_compound_risk_review_only` with at least three signal families.
- SECOP I historical exposure must be celebrated, contractor must be a canonical NIT, total representative rollup value must be at least COP 5B, and the rollup must have material additions, at least five direct/exception contracts, or at least ten legacy contracts.
- Exclude rows where the current company NIT is one of the representative's SECOP I legacy contractor NITs, keeping this detector focused on hidden person bridges rather than same-NIT continuity.
- Bound the representative bridge to at most five current-risk companies.

Implemented evidence:

- `secop_i_legacy_representative_current_risk_review_only` had 13 reviewer-only
  rows across 12 representatives and 13 current companies in the selected run;
  after shared-representative same-buyer support refresh the current table/full
  run emits 14 rows.
- Selected-run severity split: 8 critical and 5 high.
- Historical exposure totals: COP 361.52B SECOP I contract value and COP 45.39B additions.
- Current compound-risk context: 1 row has five current signal families, 5 have four, and 7 have three.
- All selected-run rows have exact representative bridge, different
  legacy/current company, COP 5B+ legacy value, material
  additions/direct-or-repeated exposure, current compound-risk, and bounded
  bridge flags.
- Selected materialization run `phase-missing-patterns-secop-i-representative-current-risk-20260606` produced 13 hits and 338 evidence rows.
- Four refreshed rows now carry
  `procurement_shared_representative_same_buyer_cluster_review_only` support.

False-positive controls:

- This detector does not prove ownership, beneficial control, legal disability, historical irregularity, current irregularity, contract breach, nonperformance, or corrupt intent.
- Reviewers must validate SECOP I contract files, representative role dates, RUES matricula history, current company files, and legitimate business continuity explanations before escalation.

Hidden connections:

- Representative bridge -> current execution failure, procurement competition, service-delivery, corporate-capacity, sanctions, or guarantee-policy-reuse support.
- Representative bridge -> declaration/company bridge, SIRI antecedent/procurement chronology, or same-buyer role/supplier queues when the same person document appears in public-person sources.
- Historical contractor sample -> current supplier network to identify migrations from consortiums, foundations, public companies, or former contractors into current high-risk companies.

Implementation status: reviewer-only detector `secop_i_legacy_representative_current_risk_review_only` is now wired in ETL/registry and materialized locally. Remaining extensions: RUES historical role-date validation, official-case bulletin overlays, beneficial-ownership inputs, and reviewer sampling before public use.

## Sources currently underused or missing from materialization

| source/dataset | state observed | corruption pattern unlocked |
|---|---|---|
| `gjp9-cutm` SECOP II Garantias | loaded, mapped, source-referenced, and used in guarantee/advance/execution-chain queue | false/missing/late guarantee validation and advance-payment chain |
| `iaeu-rcn6` SIRI antecedents | loaded, mapped, and reviewer queue materialized | disciplinary antecedent and ineligibility chronology |
| `fiscal_findings` (`8qxx-ubmq`) and `fiscal_responsibility` (`jr8e-e8tu`) | loaded, mapped, source-referenced, and used in fiscal chronology queue | Contraloria fiscal finding/responsibility to later procurement exposure |
| `secop_interadmin_agreements` | active mirror `s484-c9k3` loaded and reviewer queue materialized | mixed fund/interadmin executor chains |
| `secop_i_historical_processes` (`qddk-cgux`) | loaded, mapped, source-referenced, and used in SECOP I legacy/current-risk bridges | historical supplier and legal-representative exposure joined to current exact-NIT compound-risk evidence |
| `secop_budget_commitments`, `secop_budget_items`, `secop_cdp_requests` | loaded, mapped, source-referenced, and used in budget-chain reconciliation queue | budget-chain leakage |
| `ibyt-yi2f` SECOP II invoices | loaded, mapped, source-referenced, and used in invoice-budget reconciliation queue | invoice overruns, post-end invoice rows, and contract-summary mismatches |
| `uymx-8p3j` SECOP II payment plans | loaded, mapped, source-referenced, and used in payment-plan reconciliation queue | real-payment timing, duplicate CUFE, supplier-document mismatch, and contract/payment summary gaps |
| `health_providers` (`c36g-9fc2`) | loaded, mapped, source-referenced, and used in health/PAE service-delivery queue | health/IPS/PAE service-delivery fraud |
| MEN PAE indicators (`epkg-mphw`) | loaded, mapped, source-referenced, and used in PAE beneficiary-territory queue | municipal beneficiary/population context for PAE delivery-risk contract review |
| DNP executor/location/beneficiary sources (`epzv-8ck4`, `xikz-44ja`, `iuc2-3r6h`, `tmmn-mpqc`) | loaded, mapped, source-referenced, and used in DNP/SGR beneficiary-delivery queue | exact-BPIN beneficiary/location/demographic context on royalty-project delivery-risk queues |
| `asset_disclosures` (`8tz7-h3eu`) and conflict/SIGEP declaration sources | loaded and used in declaration chronology and declaration-company bridge | declaration/private-interest chronology and company-bridge extension |
| `tvec_orders_consolidated` (`3hdv-smhz`) | loaded, one supplier-capture signal | same-item/unit overpayment and price-dispersion review queue |
| `company_branches_nb3d` | enrichment only, no signal | shell/branch networks and domicile moves |
| `igac_property_transactions` | enrichment only, no materialized signal | property acquisition after award/sanction |
| `official_case_bulletins` | configured optional, not loaded | official case validation overlay |
| `rub_beneficial_owners` | configured reviewer-only, no source | beneficial ownership conflict |

## Dataset-family coverage audit

This table groups the broader dataset catalog into corruption-detection families. It is intentionally conservative: exact identity, money-flow, contract, sanction, or project keys get higher confidence than territory-only context.

| dataset family | current state | best missing connection | confidence/readiness |
|---|---|---|---|
| SECOP II contracts/processes/offers | loaded and heavily materialized; same-buyer role and contract modification-ladder queues now exist | competition plus role/payment/modification-ladder calibration | P0 review now |
| SECOP I historical processes | loaded and now joined to current compound-risk evidence by supplier NIT and legal-representative document | legacy supplier/representative exposure, additions, direct/special-regime modalities, and present-day risk-family convergence | P1 reviewer-only; needs corporate-history, representative role-date validation, and source-file sampling |
| Asset/conflict declarations and SIGEP | loaded; declaration chronology and declaration-company bridge queues exist | declaration chronology, private interest escalation, sensitive-position supplier/company chronology | P0 ready now, reviewer-only |
| Cuentas Claras | loaded, donor overlap and same-jurisdiction term queue exist | election-result/campaign-cap validation and legal classification | P0/P1 depending on election/cap inputs |
| TVEC item purchases | loaded, supplier-capture signal exists | same-item/unit price dispersion and buyer overpayment | P0 reviewer-only |
| RUES company registry | loaded; shared-officer, shared-representative same-buyer, identity-inconsistency, and supplier-capacity/status queues exist | CIIU/object mismatch, branch/domicile movement, and financial-capacity scoring | P1; branch/financial sources not loaded |
| SECOP guarantees, CDP, commitments, invoices, payment plans | guarantees, CDP, commitments, rubros, invoices, and payment plans are loaded and joined; advance/execution, guarantee-policy reuse, budget-chain, invoice-budget, and payment-plan queues exist | invoice/payment timing, budget-chain leakage, and guarantee-validity calibration | P0 review now; P1 for SIIF/treasury/insurer validation |
| SIRI, PACO, fiscal responsibility/findings, SECOP sanctions | PACO, SECOP sanctions, SIRI, Hallazgos Fiscales, and Responsabilidad Fiscal are loaded; SECOP sanction later-awards, SIRI chronology, and fiscal chronology queues exist | active ineligibility/antecedent/fiscal chronology across supplier, representative, role, donor | P0 for review now; P1 for legal-finality/current-disability extension |
| BPIN/SGR/DNP projects | BPIN/SGR core and DNP executor/location/beneficiary sources are loaded; SGR/OCAD executor-capacity and DNP/SGR beneficiary-delivery queues exist | DNP basic project data, official-case overlays, downstream subcontract paths, and financial-capacity evidence | P1 reviewer-only; public use needs case/file validation |
| Interadministrative agreements and special executors | active mirror loaded; exact-NIT recipient risk queue exists | agreement bridge to mixed funds, executors, subcontract/supplier concentration | P1 reviewer-only; downstream chain still needs stronger subcontract/project joins |
| Health providers and PAE context | REPS health providers and MEN PAE indicators loaded; health/PAE and PAE beneficiary-territory queues exist | IPS/PAE billed services versus beneficiaries/execution/supervision | P1 after direct school/service and health service-line inputs |
| Official bulletins, judicial decisions, actos, gacetas, control politico | configured optional, not loaded | official validation overlay for candidate cases | P1/P2 validation layer, not standalone |
| Property, beneficial ownership, cadastral/address datasets | configured optional or enrichment only, not loaded | unexplained property acquisition, beneficial-owner conflicts, vendor address anomalies | reviewer-only; high privacy/legal controls |
| PGN/PGN budget execution and NIIF/Supersoc company financials | cataloged/backlog placeholders | corporate financial capacity versus award size and public-budget flow | P1/P2 after source maps |
| DANE poverty/IPM, PDET/PNIS, UNGRD victims, reincorporation, mining/ANLA/UPME, Mindeporte | configured as optional context, mostly not loaded | territory/program-specific risk overlays | context only unless joined to exact contract/project/person evidence |

## Recommended implementation order

1. Review `procurement_role_supplier_same_buyer_review_only`: exact SECOP role document to same-buyer supplier, with person-document filters and reviewer-only output. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling and false-positive calibration.
2. Review `public_declaration_supplier_chronology_review_only`: exact declaration/sensitive-position document to later SECOP natural-person supplier exposure, same-entity emphasis, and reviewer-only public policy. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling, cap calibration, and RUES company-bridge extension.
3. Review `public_declaration_company_bridge_current_risk_review_only`: exact declaration/SIGEP person documents bridged through RUES legal-representative records to current compound-risk company NITs. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling, role-date validation, and beneficial-ownership extension.
4. Review `tvec_item_price_dispersion_review_only`: item/unit/quantity/year comparator p95 outliers with reviewer-only public policy. The detector is now wired in ETL/registry and materialized locally; next work is sample review, item normalization calibration, and joins to supplier concentration/sanctions/suspension/modification signals.
5. Review `cuentas_claras_donor_ineligibility_review`: exact donor/supplier matches during the inferred 2020-2023 local term and same mayor/governor jurisdiction, with `missing_campaign_cap` and `missing_election_result` status. The detector is now wired in ETL/registry and materialized locally; next work is legal-input loading and reviewer sampling.
6. Review `procurement_secop_sanction_later_awards_review_only`: exact SECOP sanction-contract to exact later supplier awards, with sanction finality and legal ineligibility explicitly unproven. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling and joins to same-buyer role/declaration/donor queues.
7. Review `fiscal_procurement_chronology_review_only`: exact Contraloria fiscal finding/responsibility NITs followed by later SECOP supplier exposure. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling and legal-finality/current-disability calibration before any public use.
8. Review `procurement_guarantee_advance_execution_chain`: high advance or ended pending execution plus suspension/modification/execution-delay/SECOP-sanction chain, now with SECOP guarantee status/date/value evidence. The detector is wired in ETL/registry and materialized locally; next work is reviewer sampling, guarantee-validity semantics, and invoice/payment extension.
9. Review `procurement_guarantee_policy_reuse_review_only`: exact SECOP guarantee insurer/policy reuse across different suppliers and buyers, with accepted/expired records and high aggregate exposure. The detector is wired in ETL/registry and materialized locally; next work is reviewer sampling, insurer confirmation workflow, and calibration of legitimate master-policy explanations.
10. Review `rues_supplier_capacity_status_review_only`: exact NIT supplier rows combining RUES inactive/stale/recent/multiple-registration flags with SECOP exposure. The detector is now wired in ETL/registry and materialized locally; next work is sample review and status-event normalization.
11. Review `sgr_ocad_executor_capacity_gap`: exact BPIN projects combining low SGR execution, SGR payments, linked SECOP value, procurement-failure chains, RUES capacity flags, and OCAD Paz context. The detector is now wired in ETL/registry and materialized locally; next work is sample review, financial-capacity evidence, and priority-context calibration.
12. Review `dnp_sgr_beneficiary_delivery_gap_review_only`: exact BPIN DNP executor/location/beneficiary/demographic context layered on the SGR low-execution/high-procurement queue. The detector is now wired in ETL/registry and materialized locally; next work is sample review and beneficiary-context calibration before any delivery or harm claim.
13. Review `cross_signal_compound_risk_review_only`: exact company/person identities where independent signal families converge. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling and family-combo calibration before using it as a case-prioritization layer.
14. Review `siri_antecedent_procurement_chronology_review_only`: exact SIRI person documents to supplier, RUES legal-representative, supervisor, or spending-orderer procurement exposure, with active status inferred from SIRI legal-effects and duration fields. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling, legal-finality validation, and donor/declaration/fiscal extensions.
15. Review `procurement_budget_chain_reconciliation_review_only`: exact contract ID joins across SECOP contracts, CDPs, commitments, rubros, and guarantee/execution-chain evidence. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling and source-value calibration.
16. Review `procurement_invoice_budget_reconciliation_review_only`: exact contract ID joins across SECOP contracts, invoices, budget-chain evidence, and guarantee/execution-chain evidence. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling and invoice/source-value calibration.
17. Review `procurement_payment_plan_reconciliation_review_only`: exact contract ID joins across SECOP contracts, payment plans, invoices, budget-chain evidence, and guarantee/execution-chain evidence. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling, CUFE/status calibration, and SIIF/treasury validation.
18. Review `procurement_related_bidders_same_process_review_only`: exact SECOP process winner and competing offerer sharing a RUES representative. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling of business-group, insurance, consortium, and broad-tender explanations.
19. Review `procurement_shared_representative_same_buyer_cluster_review_only`: exact RUES representative connected to multiple supplier companies awarded by the same buyer in the same year. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling, buyer-market calibration, and role-date/beneficial-control validation.
20. Review `secop_interadmin_executor_network_review_only`: exact SECOP interadministrative agreement recipient NITs with independent procurement/capacity/execution/sanction evidence. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling and stronger downstream subcontract/BPIN/SGR linkage.
21. Review `pae_beneficiary_territory_delivery_gap_review_only`: exact supplier rows where a PAE-keyword service-delivery risk contract has MEN municipal beneficiary/population context. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling, same-year/prior-year calibration, and direct school/service evidence joins.
22. Review `secop_i_legacy_supplier_current_risk_review_only`: exact-NIT SECOP I historical supplier exposure joined to current compound-risk entities. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling, corporate-history continuity checks, and official-case overlay.
23. Review `secop_i_legacy_representative_current_risk_review_only`: exact SECOP I legal-representative document bridged through current RUES representation to a different current compound-risk company. The detector is now wired in ETL/registry and materialized locally; next work is reviewer sampling, RUES role-date validation, and beneficial-ownership/corporate-history overlay.
24. Promote remaining health/PAE service-delivery extensions with direct school/service evidence and health service-line authorization inputs.

## Notes on public safety

- Role overlap, declaration chronology, RUES capacity, interadministrative-executor, TVEC price, SIRI, fiscal chronology, property, and beneficial-ownership patterns should start reviewer-only. They use exact person/company identities or price/capacity assumptions that can surface legitimate relationships.
- Public-safe outputs should aggregate by territory/project or require official public-source validation.
- Every detector should emit `what_is_unproven`: e.g., an exact overlap proves shared document identity, not corrupt intent.
- Colombia-specific legal rules matter. Donor ineligibility must use the elected office, jurisdiction, period, and legal thresholds, not just a donor/supplier match.
