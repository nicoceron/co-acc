# Anomaly Model Runbook

The Phase 13 anomaly slice trains and scores from the local parquet lake. It
does not require Neo4j.

## Train And Score

Run the full bounded workflow:

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl model train anomaly
```

This command:

- builds `lake/curated/anomaly_features/run_id=<run>/part-00000.parquet`
- trains an Isolation Forest model from a deterministic bounded sample
- trains a supervised `HistGradientBoostingClassifier` top-up when
  sanctioned-supplier weak labels have both positive and negative examples
- writes `lake/models/anomaly/<run>/iforest.joblib`
- writes `lake/models/anomaly/<run>/supervised_hgb.joblib` when the top-up is
  enabled
- scores every feature row in batches to
  `lake/curated/anomaly_scores/run_id=<run>/`
- writes `lake/models/anomaly/<run>/metrics.json`
- promotes the model by updating `lake/models/anomaly/current.json`

## API Consumption

When `lake/models/anomaly/current.json` points at a score run with parquet
under `lake/curated/anomaly_scores/run_id=<run>/`, the API can serve those
scores without Neo4j:

```bash
curl http://localhost:8000/api/v1/cases/
curl http://localhost:8000/api/v1/entity/company:9001234568/anomaly-scores?limit=25
```

`/api/v1/cases/` returns top contract anomalies as case summaries with an
`anomaly_score` object. `/api/v1/cases/{case_id}` returns the same score plus a
SECOP evidence reference. Lake-backed signal cases also attach a matching
`anomaly_score` when the signal scope can be resolved to a scored contract id.

Use deterministic run ids for repeatable operator runs:

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl model train anomaly \
  --run-id phase13-local-20260601 \
  --max-training-rows 200000 \
  --batch-size 100000
```

## Separate Steps

Build features only:

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl model build-features anomaly
```

Score with the promoted model:

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl model predict anomaly
```

Evaluate an existing score run:

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl model evaluate anomaly <score-run-id>
```

Promote an existing model run:

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl model promote anomaly <run-id>
```

## Current Scope

The current model combines an unsupervised Isolation Forest baseline with a
supervised histogram-gradient-boosting top-up. It uses the curated contract
award fact table plus PACO-backed sanctioned-supplier signal features for weak
labels and evaluation. `prior_sanction_supplier` is retained in feature and
score parquet for auditability, but it is excluded from the model input feature
tuple to avoid label leakage.

`single_bidder` is derived from `secop_offers` / `wi7w-2nvm` when that raw
source exists in the lake. The builder counts distinct effective offers by
`id_del_proceso_de_compra` and flags processes with one offer. Runs without
offers data keep the flag false. Score consumers should treat
`score_confidence=low` as a cold-start flag for suppliers with fewer than five
prior contracts.

## Local Smoke

On 2026-06-01, the local command:

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl model train anomaly \
  --run-id phase13-supervised-smoke-20260601 \
  --max-training-rows 200000 \
  --batch-size 250000
```

trained on 200,000 sampled feature rows and scored 5,442,058 contracts. The
label-derived evaluation found 12,376 sanctioned-supplier positives,
`precision_at_100=0.68`, and `holdout_precision_at_100=0.67` across 1,019,896
holdout-scored rows. This clears the Phase 13 supervised precision target.
