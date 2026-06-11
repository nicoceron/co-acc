# Curated Lake Runbook

The curated layer is built from local parquet under `lake/raw/` with DuckDB.
It is the first downstream source of truth for signal features and API data
projection; Neo4j remains optional.

## Command

Build every shipped curated table:

```bash
make curate
# equivalent explicit form:
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl curate --all
```

Build one table:

```bash
make curate TABLE=signal_feature_procurement_sanctioned_supplier_awarded
make curate TABLE=signal_feature_procurement_secop_sanction_later_awards_review_only
make curate TABLE=signal_feature_fiscal_procurement_chronology_review_only
make curate TABLE=signal_feature_siri_antecedent_procurement_chronology_review_only
make curate TABLE=signal_feature_procurement_large_modifications
make curate TABLE=signal_feature_procurement_contract_modification_ladder_review_only
make curate TABLE=signal_feature_procurement_supplier_concentration_across_entities
make curate TABLE=signal_feature_procurement_contract_value_outlier_by_category
make curate TABLE=signal_feature_procurement_repeat_awards_same_supplier
make curate TABLE=signal_feature_procurement_buyer_supplier_network_density
make curate TABLE=signal_feature_procurement_cartel_risk_cobidding
make curate TABLE=signal_feature_procurement_related_bidders_same_process_review_only
make curate TABLE=signal_feature_procurement_shared_representative_same_buyer_cluster_review_only
make curate TABLE=signal_feature_procurement_payment_plan_anomalies
make curate TABLE=signal_feature_procurement_guarantee_advance_execution_chain
make curate TABLE=signal_feature_procurement_guarantee_policy_reuse_review_only
make curate TABLE=signal_feature_procurement_budget_chain_reconciliation_review_only
make curate TABLE=signal_feature_procurement_invoice_budget_reconciliation_review_only
make curate TABLE=signal_feature_procurement_payment_plan_reconciliation_review_only
make curate TABLE=signal_feature_health_pae_service_delivery_gap_review_only
make curate TABLE=signal_feature_pae_beneficiary_territory_delivery_gap_review_only
make curate TABLE=signal_feature_procurement_contract_suspensions
make curate TABLE=signal_feature_procurement_public_servant_conflict_disclosure_overlap
make curate TABLE=signal_feature_procurement_role_supplier_same_buyer_review_only
make curate TABLE=signal_feature_public_declaration_supplier_chronology_review_only
make curate TABLE=signal_feature_public_declaration_company_bridge_current_risk_review_only
make curate TABLE=signal_feature_cuentas_claras_donor_ineligibility_review
make curate TABLE=signal_feature_pida_full30_meta
make curate TABLE=signal_feature_pida5_pida27_pida4_chain
make curate TABLE=signal_feature_project_bpin_procurement_overlap
make curate TABLE=signal_feature_project_regalias_execution_procurement_overlap
make curate TABLE=signal_feature_sgr_ocad_executor_capacity_gap
make curate TABLE=signal_feature_dnp_sgr_beneficiary_delivery_gap_review_only
make curate TABLE=signal_feature_bpin_dnp_vs_pida27_obras_prioritarias
make curate TABLE=signal_feature_tvec_multi_entity_capture
make curate TABLE=signal_feature_tvec_item_price_dispersion_review_only
make curate TABLE=signal_feature_procurement_related_companies_shared_officer
make curate TABLE=signal_feature_rues_supplier_capacity_status_review_only
make curate TABLE=signal_feature_cross_signal_compound_risk_review_only
make curate TABLE=signal_feature_secop_i_legacy_supplier_current_risk_review_only
make curate TABLE=signal_feature_secop_i_legacy_representative_current_risk_review_only
make curate TABLE=signal_feature_secop_interadmin_executor_network_review_only
make curate TABLE=dim_company
make curate TABLE=dim_buyer
make curate TABLE=dim_person
```

Validate the shipped curated table contracts against the local parquet:

```bash
make curated-contracts
make curated-contracts TABLE=dim_company
```

`LAKE_ROOT` defaults to `./lake`. Override it when running against another
lake:

```bash
make curate LAKE_ROOT=/path/to/lake
```

## Current Outputs

- `lake/curated/table=dim_subject_document/`
- `lake/curated/table=dim_company/`
- `lake/curated/table=dim_buyer/`
- `lake/curated/table=dim_person/`
- `lake/curated/table=fct_procurement_contract_awards/`
- `lake/curated/table=signal_feature_procurement_single_bidder_high_value/`
- `lake/curated/table=signal_feature_procurement_large_modifications/`
- `lake/curated/table=signal_feature_procurement_contract_modification_ladder_review_only/`
- `lake/curated/table=signal_feature_procurement_sanctioned_supplier_awarded/`
- `lake/curated/table=signal_feature_procurement_secop_sanction_later_awards_review_only/`
- `lake/curated/table=signal_feature_fiscal_procurement_chronology_review_only/`
- `lake/curated/table=signal_feature_siri_antecedent_procurement_chronology_review_only/`
- `lake/curated/table=signal_feature_procurement_supplier_concentration_across_entities/`
- `lake/curated/table=signal_feature_procurement_contract_value_outlier_by_category/`
- `lake/curated/table=signal_feature_procurement_repeat_awards_same_supplier/`
- `lake/curated/table=signal_feature_procurement_buyer_supplier_network_density/`
- `lake/curated/table=signal_feature_procurement_cartel_risk_cobidding/`
- `lake/curated/table=signal_feature_procurement_related_bidders_same_process_review_only/`
- `lake/curated/table=signal_feature_procurement_shared_representative_same_buyer_cluster_review_only/`
- `lake/curated/table=signal_feature_procurement_payment_plan_anomalies/`
- `lake/curated/table=signal_feature_procurement_guarantee_advance_execution_chain/`
- `lake/curated/table=signal_feature_procurement_guarantee_policy_reuse_review_only/`
- `lake/curated/table=signal_feature_procurement_budget_chain_reconciliation_review_only/`
- `lake/curated/table=signal_feature_procurement_invoice_budget_reconciliation_review_only/`
- `lake/curated/table=signal_feature_procurement_payment_plan_reconciliation_review_only/`
- `lake/curated/table=signal_feature_health_pae_service_delivery_gap_review_only/`
- `lake/curated/table=signal_feature_pae_beneficiary_territory_delivery_gap_review_only/`
- `lake/curated/table=signal_feature_procurement_contract_suspensions/`
- `lake/curated/table=signal_feature_procurement_short_bidding_window/`
- `lake/curated/table=signal_feature_procurement_offers_competition_drop/`
- `lake/curated/table=signal_feature_procurement_public_servant_conflict_disclosure_overlap/`
- `lake/curated/table=signal_feature_procurement_role_supplier_same_buyer_review_only/`
- `lake/curated/table=signal_feature_public_declaration_supplier_chronology_review_only/`
- `lake/curated/table=signal_feature_public_declaration_company_bridge_current_risk_review_only/`
- `lake/curated/table=signal_feature_cuentas_claras_donor_supplier_overlap/`
- `lake/curated/table=signal_feature_cuentas_claras_donor_ineligibility_review/`
- `lake/curated/table=signal_feature_pida_full30_meta/`
- `lake/curated/table=signal_feature_pida5_pida27_pida4_chain/`
- `lake/curated/table=signal_feature_project_bpin_procurement_overlap/`
- `lake/curated/table=signal_feature_project_regalias_execution_procurement_overlap/`
- `lake/curated/table=signal_feature_sgr_ocad_executor_capacity_gap/`
- `lake/curated/table=signal_feature_dnp_sgr_beneficiary_delivery_gap_review_only/`
- `lake/curated/table=signal_feature_bpin_dnp_vs_pida27_obras_prioritarias/`
- `lake/curated/table=signal_feature_tvec_multi_entity_capture/`
- `lake/curated/table=signal_feature_tvec_item_price_dispersion_review_only/`
- `lake/curated/table=signal_feature_procurement_politically_exposed_position_supplier_overlap/`
- `lake/curated/table=signal_feature_procurement_related_companies_shared_officer/`
- `lake/curated/table=signal_feature_procurement_cross_source_identity_inconsistency/`
- `lake/curated/table=signal_feature_rues_supplier_capacity_status_review_only/`
- `lake/curated/table=signal_feature_cross_signal_compound_risk_review_only/`
- `lake/curated/table=signal_feature_secop_i_legacy_supplier_current_risk_review_only/`
- `lake/curated/table=signal_feature_secop_i_legacy_representative_current_risk_review_only/`
- `lake/curated/table=signal_feature_secop_interadmin_executor_network_review_only/`
- `lake/meta/curated/<timestamp>.json`

The full default builder requires:

- `secop_ii_contracts`, resolved from raw source `jbjy-vk9h`
- `secop_ii_processes`, resolved from raw source `p6dx-8zbt`
- `secop_offers`, resolved from raw source `wi7w-2nvm`
- `secop_contract_modifications`, resolved from raw source `u8cx-r425`
- `secop_contract_suspensions`, resolved from raw source `u99c-7mfm`
- `secop_contract_execution`, resolved from raw source `mfmm-jqmq`
- `secop_guarantees`, resolved from raw source `gjp9-cutm`
- `secop_invoices`, resolved from raw source `ibyt-yi2f`
- `secop_payment_plans`, resolved from raw source `uymx-8p3j`
- `secop_cdp_requests`, resolved from raw source `a86w-fh92`
- `secop_budget_commitments`, resolved from raw source `skc9-met7`
- `secop_budget_items`, resolved from raw source `cwhv-7fnp`
- `secop_suppliers`, resolved from raw source `qmzu-gj57`
- `conflict_disclosures`, resolved from raw source `gbry-rnq4`
- `cuentas_claras_income_2019`, resolved from raw source `jgra-rz2t`
- `siri_antecedents`, resolved from raw source `iaeu-rcn6`
- `secop_integrado`, resolved from raw source `rpmr-utcd`
- `secop_i_historical_processes`, resolved from raw source `qddk-cgux`
- `secop_interadmin_agreements`, resolved from raw source `s484-c9k3`
- `secop_sanctions`, resolved from raw source `it5q-hg94`
- `fiscal_findings`, resolved from raw source `8qxx-ubmq`
- `fiscal_responsibility`, resolved from raw source `jr8e-e8tu`
- `health_providers`, resolved from raw source `c36g-9fc2`
- `pae_indicators`, resolved from raw source `epkg-mphw`
- `secop_process_bpin`, resolved from raw source `d9na-abhe`
- `sgr_expense_execution`, resolved from raw source `qkv4-ek54`
- `sgr_projects`, resolved from raw source `mzgh-shtp`
- `dnp_project_executors`, resolved from raw source `epzv-8ck4`
- `dnp_project_locations`, resolved from raw source `xikz-44ja`
- `dnp_project_beneficiary_locations`, resolved from raw source `iuc2-3r6h`
- `dnp_project_beneficiary_characterization`, resolved from raw source `tmmn-mpqc`
- `tvec_orders_consolidated`, resolved from raw source `3hdv-smhz`
- `paco_sanctions`
- `company_registry_c82u`, resolved from raw source `c82u-588k`
- `5u9e-g5w9` (SIGEP corruption-sensitive posts)
- `8tz7-h3eu` (asset declarations)

Table-specific builds only require their declared source inputs. For example,
`signal_feature_procurement_supplier_concentration_across_entities` and
`signal_feature_procurement_contract_value_outlier_by_category` and
`signal_feature_procurement_repeat_awards_same_supplier` and
`signal_feature_procurement_buyer_supplier_network_density` require only
`secop_ii_contracts`; `signal_feature_procurement_payment_plan_anomalies`
requires only `secop_ii_contracts` for the current contract-field partial;
`signal_feature_procurement_payment_plan_reconciliation_review_only`
requires `secop_payment_plans`, `secop_ii_contracts`, and invoice, budget,
and guarantee reviewer feature tables when context is available;
`signal_feature_fiscal_procurement_chronology_review_only` requires
`fiscal_findings`, `fiscal_responsibility`, and `secop_ii_contracts`; it
aggregates exact-NIT Contraloria fiscal finding/responsibility records to later
SECOP supplier exposure. The output is reviewer-only and does not prove current
legal disability, final fiscal liability for finding rows, contract illegality,
public-entity role misuse, or corrupt intent;
`signal_feature_health_pae_service_delivery_gap_review_only` requires
`health_providers`, `secop_ii_contracts`, and payment, budget, invoice,
guarantee, execution, suspension, modification, and sanction context. It uses
existing upstream reviewer feature parquet when present and temp views in fresh
full builds. The output is reviewer-only and does not prove false service,
overbilling, delivery failure, provider ineligibility, or corrupt intent;
`signal_feature_pae_beneficiary_territory_delivery_gap_review_only` requires
the health/PAE service-delivery queue plus `pae_indicators` (`epkg-mphw`). It
adds MEN municipal beneficiary, population-group, and rural/urban context to
PAE-keyword contracts. The output is reviewer-only and does not prove
beneficiary mismatch, non-delivery, ration quality failure, overbilling, legal
breach, or corrupt intent;
`signal_feature_procurement_large_modifications` requires
`secop_contract_modifications` and `secop_ii_contracts`;
`signal_feature_procurement_contract_modification_ladder_review_only` requires
`secop_contract_modifications`, `secop_contract_suspensions`,
`secop_contract_execution`, and `secop_ii_contracts`. It detects repeated
modification sequences with value, delay, payment-term, scope, suspension,
execution, budget, invoice, payment, competition, or same-buyer role support.
The output is reviewer-only and does not prove illegal modification, unlawful
avoidance of competition, fiscal harm, delivery failure, or corrupt intent;
`signal_feature_procurement_contract_suspensions` requires
`secop_contract_suspensions` and `secop_ii_contracts`;
`signal_feature_procurement_cartel_risk_cobidding` requires only `secop_offers` and `secop_ii_processes`;
`signal_feature_procurement_related_bidders_same_process_review_only`
requires `secop_offers`, `secop_ii_contracts`, and `company_registry_c82u`.
It flags high-value SECOP II processes where the winner and a competing
offerer share an exact RUES representative. The output is reviewer-only and
does not prove collusion, beneficial ownership, control, simulated
competition, bid suppression, legal ineligibility, breach, or corrupt intent;
`signal_feature_procurement_shared_representative_same_buyer_cluster_review_only`
requires `secop_ii_contracts` and `company_registry_c82u`. It flags exact RUES
legal representatives connected to two to six supplier companies awarded by the
same buyer in the same year, with COP 5B+ cluster exposure and direct,
high-value, or high-volume support. The output is reviewer-only and does not
prove collusion, beneficial ownership, control, simulated competition, legal
ineligibility, breach, or corrupt intent;
`signal_feature_procurement_guarantee_policy_reuse_review_only` requires
`secop_guarantees` and `secop_ii_contracts`. It flags small accepted/expired
guarantee clusters where the same normalized insurer and policy number appears
across different suppliers and different buyers with at least COP 1B aggregate
contract exposure. The output is reviewer-only and does not prove a false,
invalid, insurer-denied, unauthorized, or legally defective policy without
insurer confirmation and SECOP contract-file review;
`signal_feature_procurement_public_servant_conflict_disclosure_overlap`
requires `conflict_disclosures` and `secop_ii_contracts`;
`signal_feature_pida_full30_meta` requires `secop_integrado`;
`signal_feature_pida5_pida27_pida4_chain` requires
`secop_integrado` and `secop_sanctions`;
`signal_feature_project_bpin_procurement_overlap` requires
`secop_process_bpin` and `secop_ii_contracts`;
`signal_feature_project_regalias_execution_procurement_overlap` requires
`sgr_expense_execution`, `sgr_projects`, `secop_process_bpin`, and
`secop_ii_contracts`;
`signal_feature_dnp_sgr_beneficiary_delivery_gap_review_only` requires the
SGR/SECOP/RUES/execution chain plus DNP executor, project-location,
beneficiary-location, and beneficiary-characterization sources. It reuses the
exact-BPIN SGR capacity-gap queue and adds DNP delivery/beneficiary context.
The output is reviewer-only and does not prove beneficiary harm, non-delivery,
incorrect targeting, legal breach, or corrupt intent;
`signal_feature_bpin_dnp_vs_pida27_obras_prioritarias` requires
`secop_process_bpin` and `secop_integrado`;
`signal_feature_tvec_multi_entity_capture` requires
`tvec_orders_consolidated` and `secop_ii_contracts`;
`signal_feature_procurement_related_companies_shared_officer`
requires only `secop_ii_contracts` and `company_registry_c82u`;
`signal_feature_secop_i_legacy_supplier_current_risk_review_only` requires
`secop_i_historical_processes` plus the cross-signal compound-risk source
families. It aggregates exact-NIT SECOP I historical suppliers with COP 1B+
legacy exposure and joins them to current exact-NIT compound-risk entities. The
output is reviewer-only and does not prove management continuity, legacy/current
irregularity, legal disability, breach, nonperformance, or corrupt intent;
`signal_feature_secop_i_legacy_representative_current_risk_review_only`
requires `secop_i_historical_processes`, `company_registry_c82u`, and the
cross-signal compound-risk source families. It links exact SECOP I legal
representative documents to current RUES legal representatives of exact-NIT
compound-risk companies, requires COP 5B+ historical exposure and material
additions/direct or repeated exposure, and excludes same-NIT legacy/current
contractors. The output is reviewer-only and does not prove ownership,
beneficial control, legal disability, legacy/current irregularity, breach,
nonperformance, or corrupt intent;
`signal_feature_public_declaration_company_bridge_current_risk_review_only`
requires the declaration, SIGEP, RUES, and cross-signal source families. It
joins exact person documents from declarations or sensitive positions to exact
RUES legal-representative company NITs that already appear in current
compound-risk evidence. The output is reviewer-only and does not prove
ownership, beneficial control, undeclared interest, conflict of interest, legal
disability, contract irregularity, nonperformance, or corrupt intent.

`dim_company` and `dim_buyer` canonicalize Colombian NITs with the DIAN
MOD-11 verification-digit algorithm. `dim_person` uses cedula-style document
canonicalization and explicitly keeps `nit_canonical` empty as a guard against
mixing person and company identifiers.

Semantic source names are resolved through `docs/datasets/catalog.proven.csv`.

## API Exposure

The API signal list/detail endpoints read curated signal feature tables when
Neo4j has no matching materialized hits or is unavailable:

- `GET /api/v1/signals/`
- `GET /api/v1/signals/procurement_sanctioned_supplier_awarded`
- `GET /api/v1/signals/procurement_supplier_concentration_across_entities`
- `GET /api/v1/signals/procurement_repeat_awards_same_supplier`

Set `NEO4J_REQUIRED=false` to allow API startup without a graph. In that mode,
lake-backed health, meta, search, entity, signal, pattern, context, baseline,
and public company graph routes remain available. Legacy Cypher-only routes
still require a connected graph.

## Memory Posture

The builder uses DuckDB `read_parquet` views and `COPY (SELECT ...) TO parquet`.
It must not materialize full joins in Python. Sanction joins are performed via
small normalized match-key views so DuckDB can hash join on one key instead of
evaluating broad multi-condition joins across the full SECOP contract table.
Supplier concentration is a grouped DuckDB aggregation over SECOP contracts and
uses `COPY (SELECT ...) TO parquet`; Python only receives table row counts.
Repeat awards is likewise grouped in DuckDB by supplier document key and buyer
document key, then written directly to parquet. Contract value outliers are
computed in DuckDB against department/year/sector/modality/type peer groups
with a peer-count floor and per-group rank cap. Buyer-supplier network density
is computed from supplier/buyer pair rollups with repeated-relationship
thresholds. Co-bidding cartel risk is computed from SECOP offer process/supplier
rollups, bounded to 2-8 valid company-NIT suppliers per process before
self-joining supplier pairs. Payment-plan anomalies are computed from SECOP
contract payment fields and avoid broad joins over pending-payment sources.
Payment-plan reconciliation is computed from exact contract ID joins between
SECOP contracts and SECOP payment-plan rows, then enriched with invoice, budget,
and guarantee feature refs without materializing joins in Python.
Large modification rows are computed from SECOP modification-event values
joined back to exact-NIT SECOP II contracts and require either a large absolute
modification value or a large value-share increase.
Contract suspensions are computed from deduplicated SECOP suspension-event
fingerprints joined back to exact-NIT SECOP II contracts.
Contract execution delay rows are computed from SECOP execution item schedules
joined back to exact-NIT SECOP II contracts and require material delivery delay
or execution progress gaps on COP 100M+ contracts.
Cross-source identity inconsistencies are computed from exact-NIT joins between
RUES company registry records and SECOP supplier registry records, requiring at
least two mismatch dimensions before emitting reviewer-only hits.
Cuentas Claras donor/supplier overlaps are computed from exact company-NIT
joins between 2019 campaign-finance income records and post-2019 SECOP II
contract exposure.
Public-servant conflict-disclosure overlaps are computed from exact
person-document joins between affirmative conflict disclosures and aggregate
SECOP II person-supplier exposure, with exposure aggregated before disclosure
selection to avoid duplicated contract value across multiple declaration forms.
PIDA full30 meta rows are computed from SECOP Integrado text-category tags and
territory rollups, keeping only territories with broad multi-category exposure
and material high-value contract volume.
PIDA sanctioned-infrastructure chain rows are computed from exact contract-id
joins between SECOP Integrado contracts and SECOP II sanctions, excluding
future-dated sanction outliers before aggregating by territory.
BPIN project-procurement rows are computed from validated numeric BPIN process
links joined to exact SECOP II contract IDs, then aggregated by BPIN before the
high-value project cap is applied.
BPIN priority-work rows are computed from validated numeric BPIN links joined
to exact SECOP Integrado contract IDs, keeping only projects with multiple
priority infrastructure or public-work contract records.
SGR project/procurement rows are computed from SGR project metadata and SGR
expense-execution amounts joined to the same validated BPIN-to-SECOP contract
links, requiring both material SGR execution exposure and material procurement
exposure before emitting a public project row.
TVEC multi-entity capture rows are computed from exact supplier-NIT TVEC order
rollups joined to exact-NIT SECOP II supplier exposure, with item-level TVEC
values parsed directly from the source decimal fields.
Shared-officer clusters are deduplicated by company document and
representative document before DuckDB groups exposed suppliers into reviewer-only
clusters.
Shared-representative same-buyer clusters reuse the same exact RUES
legal-representative identity, but group by representative, buyer, and award
year so reviewers can see multiple supplier companies awarded by one buyer in a
bounded period.
SECOP I legacy/current-risk rows are computed by aggregating exact-NIT SECOP I
historical supplier exposure before joining to the current cross-signal
compound-risk queue; evidence refs are capped to ranked legacy records plus the
current compound-risk feature refs.
Declaration-company bridge rows are computed from exact person-document
declaration/SIGEP profiles, deduplicated RUES legal-representative records, and
the current cross-signal compound-risk queue.

## Reality Notes

On the local lake after the 2026-06-06 shared-representative same-buyer
materialization work:

- `dim_subject_document`: 1,215,832 rows
- `dim_company`: 101,916 rows
- `dim_buyer`: 4,914 rows
- `dim_person`: 259,990 rows
- `fct_procurement_contract_awards`: 5,442,058 rows
- `signal_feature_procurement_single_bidder_high_value`: 61,542 rows
- `signal_feature_procurement_large_modifications`: 582 rows
- `signal_feature_procurement_contract_modification_ladder_review_only`: 320 rows
- `signal_feature_procurement_sanctioned_supplier_awarded`: 20,184 rows
- `signal_feature_procurement_supplier_concentration_across_entities`: 497 rows
- `signal_feature_procurement_contract_value_outlier_by_category`: 2,022 rows
- `signal_feature_procurement_repeat_awards_same_supplier`: 9,716 rows
- `signal_feature_procurement_buyer_supplier_network_density`: 364 rows
- `signal_feature_procurement_cartel_risk_cobidding`: 866 rows
- `signal_feature_procurement_related_bidders_same_process_review_only`: 16 rows
- `signal_feature_procurement_shared_representative_same_buyer_cluster_review_only`: 166 rows
- `signal_feature_procurement_payment_plan_anomalies`: 375 rows
- `signal_feature_procurement_guarantee_policy_reuse_review_only`: 723 rows
- `signal_feature_procurement_contract_suspensions`: 10,460 rows
- `signal_feature_procurement_contract_execution_delay`: 48 rows
- `signal_feature_procurement_short_bidding_window`: 90,592 rows
- `signal_feature_procurement_offers_competition_drop`: 13 rows
- `signal_feature_procurement_public_servant_conflict_disclosure_overlap`: 65 rows
- `signal_feature_cuentas_claras_donor_supplier_overlap`: 533 rows
- `signal_feature_pida_full30_meta`: 93 rows
- `signal_feature_pida5_pida27_pida4_chain`: 38 rows
- `signal_feature_project_bpin_procurement_overlap`: 654 rows
- `signal_feature_project_regalias_execution_procurement_overlap`: 41 rows
- `signal_feature_bpin_dnp_vs_pida27_obras_prioritarias`: 796 rows
- `signal_feature_tvec_multi_entity_capture`: 87 rows
- `signal_feature_procurement_politically_exposed_position_supplier_overlap`: 284 rows
- `signal_feature_procurement_related_companies_shared_officer`: 1,482 rows
- `signal_feature_procurement_cross_source_identity_inconsistency`: 50 rows

Additional 2026-06-05 missing-pattern feature-table builds produced:

- `signal_feature_procurement_role_supplier_same_buyer_review_only`: 11,311 rows
- `signal_feature_public_declaration_supplier_chronology_review_only`: 25,000 rows
- `signal_feature_public_declaration_company_bridge_current_risk_review_only`: 56 rows
- `signal_feature_tvec_item_price_dispersion_review_only`: 4,601 rows
- `signal_feature_cuentas_claras_donor_ineligibility_review`: 322 rows
- `signal_feature_procurement_secop_sanction_later_awards_review_only`: 127 rows
- `signal_feature_fiscal_procurement_chronology_review_only`: 80 rows
- `signal_feature_siri_antecedent_procurement_chronology_review_only`: 1,120 rows
- `signal_feature_procurement_guarantee_advance_execution_chain`: 257 rows
- `signal_feature_procurement_budget_chain_reconciliation_review_only`: 1,000 rows
- `signal_feature_procurement_invoice_budget_reconciliation_review_only`: 376 rows
- `signal_feature_procurement_payment_plan_reconciliation_review_only`: 1,000 rows
- `signal_feature_health_pae_service_delivery_gap_review_only`: 639 rows
- `signal_feature_pae_beneficiary_territory_delivery_gap_review_only`: 64 rows
- `signal_feature_rues_supplier_capacity_status_review_only`: 1,044 rows
- `signal_feature_sgr_ocad_executor_capacity_gap`: 60 rows
- `signal_feature_dnp_sgr_beneficiary_delivery_gap_review_only`: 60 rows
- `signal_feature_cross_signal_compound_risk_review_only`: 1,000 rows
- `signal_feature_secop_i_legacy_supplier_current_risk_review_only`: 556 rows
- `signal_feature_secop_i_legacy_representative_current_risk_review_only`: 14 rows;
  selected run
  `phase-missing-patterns-secop-i-representative-current-risk-20260606`
  produced 13 hits and 338 evidence rows; after shared-representative
  same-buyer support refresh the current table/full run emits 14 hits.
- `signal_feature_procurement_contract_modification_ladder_review_only`: 320 rows
- `signal_feature_secop_interadmin_executor_network_review_only`: 702 rows
- `signal_feature_procurement_related_bidders_same_process_review_only`: 16 rows;
  selected run `phase-missing-patterns-related-bidders-20260605` produced
  16 hits and 226 evidence rows.
- `signal_feature_procurement_guarantee_policy_reuse_review_only`: 723 rows;
  selected run `phase-missing-patterns-guarantee-policy-reuse-20260605`
  produced 723 hits and 3,110 evidence rows. After refreshing bridge queues,
  43 cross-signal rows, 64 interadministrative rows, 11 SECOP I legacy/current
  rows, and 3 declaration-company bridge rows carry guarantee-policy-reuse
  support.
- `signal_feature_procurement_shared_representative_same_buyer_cluster_review_only`:
  selected run `phase-missing-patterns-shared-representative-same-buyer-20260606`
  produced 166 hits and 1,706 evidence rows. After refreshing bridge queues,
  26 cross-signal rows, 10 SECOP I legacy/current supplier rows, and 4 SECOP I
  legal-representative/current-risk rows carry shared-representative same-buyer
  support.

The first signal feature table joined 13,423 distinct SECOP contracts, 664
distinct supplier document keys, and PACO evidence from `multas_secop`,
`antecedentes_siri_sanciones`, `colusiones_en_contratacion`, and
`responsabilidades_fiscales`.

The supplier concentration feature emits supplier-level rows when a supplier
has at least 25 contracts, 50 distinct public buyers, and COP 1B total awarded
value in SECOP II contracts. Evidence refs are the top SECOP process URLs by
contract value.

The repeat-awards feature emits buyer/supplier pair rows when the same buyer
awards at least 10 contracts and COP 1B total value to the same supplier.
Evidence refs are the top SECOP process URLs by contract value.

The contract value outlier feature emits reviewer-only contract rows when a
contract is among the top 3 high-value contracts in a department/year/sector/
modality/type peer group with at least 100 contracts, is at least COP 5B, and is
statistically extreme against the peer median, standard deviation, or IQR.

The buyer-supplier network density feature emits reviewer-only supplier rows
when exact-NIT suppliers have at least 10 buyers, 8 repeated buyer pairs, 55%
of contracts in repeated relationships, and COP 2B total exposure.

The BPIN project feature emits public project rows when validated SECOP
process-to-BPIN links join to at least two SECOP II contracts and COP 100B total
contract value. The 2026-06-05 local build produced 654 rows, including 82 high
severity rows and 572 medium severity rows.

The BPIN priority-work feature emits public project rows when validated SECOP
process-to-BPIN links join to SECOP Integrado priority infrastructure or
public-work records with at least 3 contracts and COP 25B linked value. The
2026-06-05 local build produced 796 rows, including 604 high severity rows and
192 medium severity rows, covering 162,594 priority-work contracts and COP
125,718,370,853,838 linked value.

The SGR project/procurement feature emits public project rows when SGR project
metadata and SGR expense execution join to validated SECOP BPIN links with at
least two SECOP II contracts, COP 50B procurement value, and COP 50B SGR
execution exposure. The 2026-06-05 local build produced 41 rows, including 10
high severity rows and 31 medium severity rows.

The TVEC multi-entity capture feature emits public supplier rows when an
exact-NIT supplier appears across at least 50 TVEC buying entities, 100 TVEC
orders, COP 1B TVEC item value, 20 SECOP II contracts, and COP 5B SECOP II
contract value. The 2026-06-05 local build produced 87 rows, including 53 high
severity rows and 34 medium severity rows.

The co-bidding cartel risk feature emits reviewer-only company rows for both
companies in each repeated co-bid pair when exact non-placeholder supplier NITs
share at least 20 bounded-supplier processes, 5 buyers, and enough pair overlap
to avoid one-off high-volume market peers. Evidence refs are the top SECOP
process URLs by recency.

The payment-plan anomaly feature emits reviewer-only contract rows when an
exact-NIT supplier has a high advance-payment share, paid value materially
exceeding contract value, or invoiced value materially exceeding contract value.
Evidence refs are SECOP process URLs when available.

The contract-suspension feature emits public contract rows when deduplicated
SECOP suspension events show at least two distinct suspension dates on a COP
100M+ exact-NIT contract. Evidence refs include the SECOP process URL and
bounded suspension-event record ids.

The contract-execution-delay feature emits public contract rows when SECOP
execution item schedules show at least 30 days of delivery delay, unresolved
overdue delivery, or a material expected-vs-actual progress gap on a COP 100M+
exact-NIT contract. Evidence refs include the SECOP process URL and bounded
execution-item record ids.

The PIDA full30 meta feature emits public territory rows when SECOP Integrado
records span at least 12 PIDA-style policy categories, at least 500 contracts,
at least 50 COP 1B+ contracts, and at least COP 1T total contract value. The
2026-06-05 local build produced 93 rows, including 7 high severity rows and 86
medium severity rows, covering 541,240 tagged contracts and COP
12,124,250,151,149,100 total contract value.

The shared-officer feature emits reviewer-only supplier rows when at least two
SECOP-exposed supplier companies share the same RUES legal representative, each
company has at least 3 contracts and COP 100M total awarded value, and the
company identity is an exact NIT match.

The cross-source identity-inconsistency feature emits reviewer-only supplier
rows when an exact-NIT RUES/SECOP supplier match has at least two mismatch
dimensions across normalized name, legal representative document, or active
status. Evidence refs include one RUES company record and one SECOP supplier
registry record.

The Cuentas Claras donor/supplier feature emits public company rows when an
exact-NIT campaign-finance income contributor has at least COP 1M income
reported in Cuentas Claras 2019 and at least COP 100M post-2019 SECOP II
contract exposure. Evidence refs include bounded Cuentas Claras income records
and SECOP process URLs.
