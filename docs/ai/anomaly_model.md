# CO-ACC Anomaly Model Card

## Purpose

The anomaly model scores SECOP procurement contracts from 0 to 1 so reviewers
can prioritize public-record patterns for inspection. A high score means the
contract is unusual relative to observed procurement behavior; it is not an
assertion of illegality or misconduct.

## Current Model

- Model kind: Isolation Forest + supervised histogram-gradient-boosting top-up
- Runtime: batch ETL, not request path
- Training source: `lake/curated/anomaly_features/run_id=<run>/`
- Output: `lake/curated/anomaly_scores/run_id=<run>/`
- Promotion pointer: `lake/models/anomaly/current.json`
- API surface: `/api/v1/cases/`, `/api/v1/cases/{case_id}`, and
  `/api/v1/entity/{entity_id}/anomaly-scores`

The current slice trains an unsupervised Isolation Forest baseline and, when
both positive and negative labels exist, a supervised scikit-learn
`HistGradientBoostingClassifier` top-up. PACO-backed sanctioned-supplier
overlaps are weak supervision labels and evaluation labels; they are not part
of `FEATURE_NAMES`, so the model does not consume the target label as an input.

Local smoke run `phase13-supervised-smoke-20260601` scored 5,442,058 contracts.
It reported `precision_at_100=0.68` overall and
`holdout_precision_at_100=0.67` on the deterministic sanctioned-supplier
holdout split, clearing the Phase 13 `precision@100 >= 0.4` target.

### Evaluation caution (2026-07-13)

The Phase 13 holdout is deterministic by contract-id hash, not by supplier.
Contracts for the same supplier can occur in both training and evaluation.
The reported `holdout_precision_at_100=0.67` therefore measures contract-row
separation for suppliers the model may already observe; it does **not** prove
generalization to unseen suppliers and must not be used as the contest's final
model-quality claim.

A read-only supplier-disjoint diagnostic against the existing feature parquet
held out entire canonical supplier ids. On 1,013,629 holdout contracts with
1,247 PACO-positive rows (0.123% base rate):

- the current supervised top-up produced `precision_at_100=0.00`;
- the current unsupervised component produced `precision_at_100=0.02` and
  `precision_at_1000=0.017`;
- a newly fitted nine-feature Isolation Forest that excluded the optional
  offers-derived `single_bidder` variable produced `precision_at_100=0.01`,
  `precision_at_1000=0.026`, and ROC AUC `0.735`.

The nine-feature result is about 21 times the base rate at 1,000, but it remains
a diagnostic until the supplier-disjoint split is implemented as a reproducible
repository evaluation and rerun on fresh data. The current promoted hybrid
model is not contest-release-ready.

### Contest MVP target

The contest target defined in `docs/mvp_requirements.md` keeps one Isolation
Forest but expands the input matrix to 15 variables: the nine contract-only
variables above, four near-complete process variables joined through
`id_del_portafolio`, and two deduplicated suspension-history variables. It
replaces the raw offers dependency and disables the supervised top-up. The
target is a requirement, not the current promoted implementation; fresh
supplier-disjoint results must be recorded before claiming that the added
sources improve prioritization.

Contract modifications are the only approved stretch input. After a complete
2023+ backfill, exact-key coverage validation, and supplier-disjoint ablation,
three modification variables may expand the target from 15 to 18. Until all
gates in `docs/mvp_requirements.md` pass, modifications remain signal/evidence
context and are not part of `FEATURE_NAMES`.

## Features

The feature builder uses curated lake tables and, when present, the raw
`secop_offers` source for offer-count features:

- log contract value
- buyer-relative value z-score
- modality-relative value z-score
- prior 12-month buyer/supplier concentration
- prior 12-month buyer contract count
- prior 12-month supplier contract count
- share of prior buyer spend
- buyer signing-day timing anomaly score
- modality value mismatch flag
- single-bidder flag derived from SECOP offer counts by process

`prior_sanction_supplier` remains in the feature parquet and score parquet as
the weak label/evaluation field. It is excluded from the model input feature
tuple to avoid leakage.

## Limitations

- `single_bidder` is populated only when SECOP offers are present in the lake;
  runs without that source keep the flag false rather than fabricating offer
  evidence.
- PACO-backed sanction overlaps are proxy labels for prioritization quality,
  not ground truth findings of wrongdoing.
- The current contract-hash holdout is not entity-disjoint and overstates
  generalization to suppliers absent from training.
- Cold-start suppliers have lower confidence because there is little prior
  behavior to compare against.
- Scores are prioritization signals. They must be presented with evidence and
  pattern language, not criminality language, per `ETHICS.md`.

## Operational Contract

Use `docs/runbooks/anomaly_model.md` for commands. A valid promoted run must
contain:

- `lake/models/anomaly/<run>/iforest.joblib`
- `lake/models/anomaly/<run>/supervised_hgb.joblib` when labels permit
- `lake/models/anomaly/<run>/metrics.json`
- `lake/models/anomaly/current.json`
- `lake/curated/anomaly_scores/run_id=<run>/*.parquet`

The API response embeds scores as `anomaly_score` with `contract_id`,
`entity_uid`, `score`, `score_confidence`, `top_features`, `process_url`,
`score_run_id`, `model_run_id`, `feature_run_id`, and `scored_at`.
