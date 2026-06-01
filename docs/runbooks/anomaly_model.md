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
- writes `lake/models/anomaly/<run>/iforest.joblib`
- scores every feature row in batches to
  `lake/curated/anomaly_scores/run_id=<run>/`
- writes `lake/models/anomaly/<run>/metrics.json`
- promotes the model by updating `lake/models/anomaly/current.json`

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

The current model is an unsupervised Isolation Forest. It uses the curated
contract award fact table plus PACO-backed sanctioned-supplier signal features
for label-derived evaluation. The supervised XGBoost top-up remains a later
Phase 13 slice; the current model metadata records that limitation explicitly.

`single_bidder` is emitted as `false` until offers-to-process linkage is
curated. Score consumers should treat `score_confidence=low` as a cold-start
flag for suppliers with fewer than five prior contracts.

## Local Smoke

On 2026-06-01, the local command:

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl model train anomaly \
  --run-id phase13-local-smoke-20260601 \
  --max-training-rows 5000 \
  --batch-size 500000
```

trained on 5,000 sampled feature rows and scored 5,442,058 contracts. The
label-derived evaluation found 12,376 sanctioned-supplier positives and
`precision_at_100=0.03`. That is enough to verify the batch runtime path, but
below the Phase 13 supervised precision target; the XGBoost top-up remains
required.
