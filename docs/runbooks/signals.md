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
- `lake/curated/table=signal_feature_procurement_sanctioned_supplier_awarded/`
- `lake/curated/table=signal_feature_procurement_supplier_concentration_across_entities/`
- `lake/curated/table=signal_feature_procurement_contract_value_outlier_by_category/`
- `lake/curated/table=signal_feature_procurement_repeat_awards_same_supplier/`
- `lake/curated/table=signal_feature_procurement_buyer_supplier_network_density/`
- `lake/curated/table=signal_feature_procurement_cartel_risk_cobidding/`
- `lake/curated/table=signal_feature_procurement_payment_plan_anomalies/`
- `lake/curated/table=signal_feature_procurement_contract_suspensions/`
- `lake/curated/table=signal_feature_procurement_contract_execution_delay/`
- `lake/curated/table=signal_feature_procurement_short_bidding_window/`
- `lake/curated/table=signal_feature_procurement_offers_competition_drop/`
- `lake/curated/table=signal_feature_procurement_public_servant_conflict_disclosure_overlap/`
- `lake/curated/table=signal_feature_cuentas_claras_donor_supplier_overlap/`
- `lake/curated/table=signal_feature_procurement_politically_exposed_position_supplier_overlap/`
- `lake/curated/table=signal_feature_procurement_related_companies_shared_officer/`
- `lake/curated/table=signal_feature_procurement_cross_source_identity_inconsistency/`

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

On the local lake generated during the 2026-06-05
`phase11-local-20260605-conflict-disclosures` run:

- Materialized parquet: 161,770 `signal_hits` rows and 266,715
  `evidence_bundles` rows
- `procurement_single_bidder_high_value`: 59,757 hits
- `procurement_large_modifications`: 580 hits
- `procurement_sanctioned_supplier_awarded`: 16,894 hits
- `procurement_supplier_concentration_across_entities`: 497 hits
- `procurement_contract_value_outlier_by_category`: 2,022 hits
- `procurement_repeat_awards_same_supplier`: 9,716 hits
- `procurement_buyer_supplier_network_density`: 364 hits
- `procurement_cartel_risk_cobidding`: 866 hits
- `procurement_payment_plan_anomalies`: 375 hits
- `procurement_contract_suspensions`: 10,460 hits
- `procurement_contract_execution_delay`: 48 hits
- `procurement_short_bidding_window`: 57,799 hits
- `procurement_offers_competition_drop`: 13 hits
- `procurement_public_servant_conflict_disclosure_overlap`: 65 hits
- `cuentas_claras_donor_supplier_overlap`: 533 hits
- `procurement_politically_exposed_position_supplier_overlap`: 249 hits
- `procurement_related_companies_shared_officer`: 1,482 hits
- `procurement_cross_source_identity_inconsistency`: 50 hits

The production public catalog currently exposes the 11 public materialized
signals and hides the 7 reviewer-only materialized signals unless a reviewer
path explicitly permits them.
