# Signal Materialization Runbook

The lake-backed signal materializer turns shipped curated signal feature
tables into run-scoped parquet outputs. It does not require Neo4j.

## Build

Rebuild every shipped signal run:

```bash
make materialize-all
# equivalent explicit form:
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl signals materialize --all
```

Materialize one signal:

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl signals materialize \
  --signal procurement_sanctioned_supplier_awarded
```

`--run-id <id>` is available for deterministic operator reruns and tests.
`--allow-empty` writes a run even when a selected signal currently emits zero
hits; without it, zero-hit selected signals fail the run.

## Inputs

The shipped materializer slice reads:

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
- `lake/curated/table=signal_feature_procurement_contract_execution_delay/`
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

Run `make curate` first when any of those feature tables are missing or stale.

## Outputs

Each run writes:

- `lake/curated/signal_hits/run_id=<run>/part-00000.parquet`
- `lake/curated/evidence_bundles/run_id=<run>/part-00000.parquet`
- `lake/meta/signal_runs/<run>.json`

`signal_hits` carries the public signal contract: run id, signal id/version,
entity uid, scope key/type, severity, score, title, description, public flags,
identity quality, timestamps, evidence bundle id, and evidence refs.

`evidence_bundles` expands each evidence ref into an auditable row with source
id, record/url, source parquet path, row selector, label, and identity metadata.
The materializer writes one row per deterministic `hit_id`; API readers also
deduplicate by `hit_id` and evidence `item_index` so repeated source feature
rows do not duplicate user-facing hits.

## Recovery

Runs are append-only by `run_id`. Reusing a `run_id` atomically replaces only
that run partition and manifest. If a run fails, inflight directories are
removed and no completed manifest is written.

Use:

```bash
make materialize-all RUN_ID=manual-rerun-20260601
```

The API reports the newest completed lake signal run from
`lake/meta/signal_runs/`, falling back to curated-table manifests when no
signal run exists.

## API Behavior

There is no feature flag required for lake signal runs. When a completed
signal-run manifest is present, `/api/v1/signals` reads counts, samples, and
evidence items from `signal_hits` and `evidence_bundles`. If no signal run is
present yet, it falls back to the shipped curated signal feature tables for
counts and samples.

When Neo4j is unavailable, `/api/v1/cases/` exposes a public-safe lake dossier
view with one case per materialized signal hit. `/api/v1/cases/{hit_id}` reads
the corresponding signal hit and evidence bundle directly from parquet.
Graph-backed case creation and refresh remain available only when Neo4j is
connected.

Use `make api-smoke` to start the API in Neo4j-off mode and verify the
lake-backed signal, search, entity, pattern, public graph, case, and
citizen-agent endpoints against the local lake.

`/api/v1/search`, `/api/v1/entity/{identifier}`, and
`/api/v1/entity/by-element-id/{element_id}` prefer curated `dim_company`,
`dim_buyer`, and `dim_person` parquet through DuckDB before falling back to
Neo4j.
`/api/v1/entity/{entity_id}/signals` reads the latest materialized signal run
and returns deduplicated signal hits plus evidence items for that entity key
before falling back to graph-stored signals.
Public-mode person/entity guards still apply before reading lake dimensions.

The frontend context routes are also lake-first.
`/api/v1/entity/{entity_id}/evidence-trail`,
`/api/v1/entity/{entity_id}/exposure`,
`/api/v1/entity/{entity_id}/timeline`, and `/api/v1/graph/{entity_id}` derive
evidence bundles, exposure factors, timeline events, and graph-shaped nodes
from the same materialized signal hits plus promoted anomaly score parquet
before falling back to Neo4j.

`/api/v1/baseline/{entity_id}` uses the curated
`fct_procurement_contract_awards` table to compute sector and regional peer
comparisons for suppliers before falling back to Neo4j. The regional baseline
uses award department/city because the lake dimension tables do not carry CIIU
or graph-only location nodes.

`/api/v1/patterns/{entity_id}` and
`/api/v1/public/patterns/company/{company_ref}` use the latest materialized
signal run for mapped signal-backed patterns, including sanctioned supplier
records, supplier concentration, recurring low-threshold awards, low competition,
short bidding windows, execution-delay indicators, campaign-finance donor
overlap, sensitive-position supplier overlap, and shared-officer supplier
networks. Public guards still hide
reviewer-only signals from public
requests. Reviewer-only materialized signals without pattern mappings are still
available through reviewer signal/case surfaces, not public pattern routes.
Legacy Cypher-only patterns still require Neo4j until their DuckDB feature
tables are shipped.

## Reality Notes

On the local lake generated during the 2026-06-06
`phase-missing-patterns-all-signals-shared-representative-20260606` run:

- Materialized parquet: 214,095 `signal_hits` rows and 581,550
  `evidence_bundles` rows
- `procurement_single_bidder_high_value`: 59,757 hits
- `procurement_large_modifications`: 580 hits
- `procurement_contract_modification_ladder_review_only`: 320 hits
- `procurement_sanctioned_supplier_awarded`: 16,894 hits
- `procurement_secop_sanction_later_awards_review_only`: 127 hits
- `fiscal_procurement_chronology_review_only`: 80 hits
- `siri_antecedent_procurement_chronology_review_only`: 1,120 hits
- `procurement_supplier_concentration_across_entities`: 497 hits
- `procurement_contract_value_outlier_by_category`: 2,022 hits
- `procurement_repeat_awards_same_supplier`: 9,716 hits
- `procurement_buyer_supplier_network_density`: 364 hits
- `procurement_cartel_risk_cobidding`: 866 hits
- `procurement_related_bidders_same_process_review_only`: 16 hits
- `procurement_shared_representative_same_buyer_cluster_review_only`: 166 hits
- `procurement_payment_plan_anomalies`: 375 hits
- `procurement_guarantee_policy_reuse_review_only`: 723 hits
- `procurement_contract_suspensions`: 10,460 hits
- `procurement_contract_execution_delay`: 48 hits
- `procurement_short_bidding_window`: 57,799 hits
- `procurement_offers_competition_drop`: 13 hits
- `procurement_public_servant_conflict_disclosure_overlap`: 65 hits
- `cuentas_claras_donor_supplier_overlap`: 533 hits
- `pida_full30_meta`: 93 hits
- `pida5_pida27_pida4_chain`: 38 hits
- `project_bpin_procurement_overlap`: 654 hits
- `project_regalias_execution_procurement_overlap`: 41 hits
- `bpin_dnp_vs_pida27_obras_prioritarias`: 796 hits
- `tvec_multi_entity_capture`: 87 hits
- `procurement_politically_exposed_position_supplier_overlap`: 249 hits
- `procurement_related_companies_shared_officer`: 1,482 hits
- `procurement_cross_source_identity_inconsistency`: 50 hits
- `public_declaration_company_bridge_current_risk_review_only`: 58 hits
- `secop_i_legacy_supplier_current_risk_review_only`: 556 hits
- `secop_i_legacy_representative_current_risk_review_only`: 14 hits

Additional 2026-06-05/2026-06-06 missing-pattern selected runs produced these
reviewer-only feature tables and materialized hits:

- `procurement_role_supplier_same_buyer_review_only`: 11,311 hits
- `public_declaration_supplier_chronology_review_only`: 25,000 hits
- `public_declaration_company_bridge_current_risk_review_only`: 56 selected-run
  hits; refreshed table/full run now emits 58 hits after guarantee-policy-reuse
  support.
- `tvec_item_price_dispersion_review_only`: 4,601 hits
- `cuentas_claras_donor_ineligibility_review`: 322 hits
- `procurement_secop_sanction_later_awards_review_only`: 127 hits
- `fiscal_procurement_chronology_review_only`: 80 hits
- `siri_antecedent_procurement_chronology_review_only`: 1,120 hits
- `procurement_guarantee_advance_execution_chain`: 257 hits
- `procurement_budget_chain_reconciliation_review_only`: 1,000 hits
- `procurement_invoice_budget_reconciliation_review_only`: 376 hits
- `procurement_payment_plan_reconciliation_review_only`: 1,000 hits
- `health_pae_service_delivery_gap_review_only`: 639 hits
- `pae_beneficiary_territory_delivery_gap_review_only`: 64 hits
- `rues_supplier_capacity_status_review_only`: 1,044 hits
- `sgr_ocad_executor_capacity_gap`: 60 hits
- `dnp_sgr_beneficiary_delivery_gap_review_only`: 60 hits
- `cross_signal_compound_risk_review_only`: 1,000 hits
- `secop_i_legacy_supplier_current_risk_review_only`: 549 selected-run hits;
  refreshed table/full run now emits 556 hits after guarantee-policy-reuse
  support.
- `secop_i_legacy_representative_current_risk_review_only`: selected run
  `phase-missing-patterns-secop-i-representative-current-risk-20260606`
  produced 13 hits and 338 evidence rows; refreshed table/full run now emits
  14 hits after shared-representative same-buyer support.
- `secop_interadmin_executor_network_review_only`: 698 selected-run hits;
  refreshed table/full run now emits 702 hits after guarantee-policy-reuse
  support.
- `procurement_contract_modification_ladder_review_only`: selected run
  `phase-missing-patterns-modification-ladder-20260605` produced 320 hits and
  2,767 evidence rows; refreshed bridge queues now carry ladder support in 126
  cross-signal rows, 161 interadministrative rows, 48 SECOP I legacy/current
  rows, and 5 declaration-company bridge rows.
- `procurement_related_bidders_same_process_review_only`: selected run
  `phase-missing-patterns-related-bidders-20260605` produced 16 hits and
  226 evidence rows; refreshed bridge queues now carry related-bidders support
  in 3 cross-signal rows, 1 interadministrative row, and 2 SECOP I
  legacy/current rows.
- `procurement_guarantee_policy_reuse_review_only`: selected run
  `phase-missing-patterns-guarantee-policy-reuse-20260605` produced 723 hits
  and 3,110 evidence rows; refreshed bridge queues now carry
  guarantee-policy-reuse support in 43 cross-signal rows, 64
  interadministrative rows, 11 SECOP I legacy/current rows, and 3
  declaration-company bridge rows.
- `procurement_shared_representative_same_buyer_cluster_review_only`: selected
  run `phase-missing-patterns-shared-representative-same-buyer-20260606`
  produced 166 hits and 1,706 evidence rows; refreshed bridge queues now carry
  this support in 26 cross-signal rows, 10 SECOP I legacy/current supplier rows,
  and 4 SECOP I legal-representative/current-risk rows.

The production public catalog exposes public-safe materialized signals and
hides reviewer-only materialized signals unless a reviewer path explicitly
permits them.
