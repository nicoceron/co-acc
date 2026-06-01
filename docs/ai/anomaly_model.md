# CO-ACC Anomaly Model Card

## Purpose

The anomaly model scores SECOP procurement contracts from 0 to 1 so reviewers
can prioritize public-record patterns for inspection. A high score means the
contract is unusual relative to observed procurement behavior; it is not an
assertion of illegality or misconduct.

## Current Model

- Model kind: Isolation Forest
- Runtime: batch ETL, not request path
- Training source: `lake/curated/anomaly_features/run_id=<run>/`
- Output: `lake/curated/anomaly_scores/run_id=<run>/`
- Promotion pointer: `lake/models/anomaly/current.json`

The current slice is unsupervised. PACO-backed sanctioned-supplier overlaps
are used for evaluation and prioritization checks, not as supervised training
labels yet. The supervised XGBoost top-up described in the post-refactor plan
remains a later Phase 13 slice.

Local smoke run `phase13-local-smoke-20260601` scored 5,442,058 contracts and
reported `precision_at_100=0.03` against sanctioned-supplier labels. This
validates the runtime path but is below the Phase 13 supervised precision
target.

## Features

The feature builder uses only curated lake tables:

- log contract value
- buyer-relative value z-score
- modality-relative value z-score
- prior 12-month buyer/supplier concentration
- prior 12-month buyer contract count
- prior 12-month supplier contract count
- share of prior buyer spend
- buyer signing-day timing anomaly score
- modality value mismatch flag
- single-bidder placeholder, currently `false`
- prior sanctioned-supplier overlap flag

## Limitations

- `single_bidder` remains unavailable until offers-to-process linkage is
  curated.
- Cold-start suppliers have lower confidence because there is little prior
  behavior to compare against.
- Scores are prioritization signals. They must be presented with evidence and
  pattern language, not criminality language, per `ETHICS.md`.

## Operational Contract

Use `docs/runbooks/anomaly_model.md` for commands. A valid promoted run must
contain:

- `lake/models/anomaly/<run>/iforest.joblib`
- `lake/models/anomaly/<run>/metrics.json`
- `lake/models/anomaly/current.json`
- `lake/curated/anomaly_scores/run_id=<run>/*.parquet`
