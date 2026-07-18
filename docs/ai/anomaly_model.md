# co/acc Isolation Forest Model Card

## Purpose

The model ranks SECOP procurement contracts from 0 to 1 so a reviewer can
inspect unusual public-record activity first. A high score means unusual in the
observed feature space. It is not a probability of corruption, an accusation,
or a legal conclusion.

## MVP model

- Model: one scikit-learn `IsolationForest`
- Runtime: deterministic batch ETL; never trained in the request path
- Random seed: recorded in `metrics.json` (default `42`)
- Training: bounded deterministic sample excluding the supplier holdout
- Inputs: `lake/curated/anomaly_features/run_id=<run>/`
- Outputs: `lake/curated/anomaly_scores/run_id=<run>/`
- Promotion pointer: `lake/models/anomaly/current.json`
- API: `/api/v1/cases/`, `/api/v1/cases/{case_id}`, and
  `/api/v1/entity/{entity_id}/anomaly-scores`

No supervised top-up, ensemble, LLM classifier, or second anomaly model is part
of the MVP path.

## Frozen MVP run

Run `mvp-iforest-20260713` used seed 42, trained on 5,000 bounded rows after
excluding the supplier holdout, and scored 5,442,058 contract rows.

Against 12,376 PACO/sanction weak-label positives (0.227% base rate), it
reported precision@100 of 0.000, precision@1,000 of 0.026, average precision of
0.0165, and ROC AUC of 0.8463. Random ranking has expected precision 0.00227.

The complete-supplier holdout contained 1,021,589 rows and 5,080 weak-label
positives (0.497% base rate). It reported precision@100 of 0.040,
precision@1,000 of 0.121, average precision of 0.0720, and ROC AUC of 0.9203.
These values assess ranking against weak labels; they do not validate corruption
detection. The weak overall precision@100 limits model claims but does not
prevent showing unsupervised anomaly distance with that limitation.

## Features

The smallest working contract schema contains:

- log contract value;
- buyer-relative and modality-relative value z-scores;
- buyer-supplier concentration;
- prior 12-month buyer and supplier contract counts;
- share of the buyer's prior 12-month spend;
- buyer signing-day timing anomaly;
- modality/value mismatch; and
- single-bidder status when the offers source is available.

The score parquet retains `prior_sanction_supplier` only as a weak evaluation
label. It is excluded from `FEATURE_NAMES` and therefore is not a model input.

## Evaluation contract

`metrics.json` records:

- evaluated rows and weak-label positives;
- weak-label base rate;
- precision at 100 and 1,000;
- average precision and ROC AUC;
- random-ranking expected precision (the base rate); and
- the same metrics for a deterministic complete-supplier holdout based on
  canonical `entity_uid` when available.

PACO and sanction matches are weak labels, not ground truth. Evaluation quality
limits the claims that can be made; it does not convert anomaly distance into a
finding of misconduct.

## Result explanations

Each score carries the three feature names with the largest standardized
deviation from the training median. The public UI translates those names into
plain Spanish and shows contract, supplier, buyer, amount, signing date, run
identifiers, scoring date, and the official SECOP link when present.

## Limitations

- Cold-start suppliers have little history and receive a lower score-confidence
  label.
- `single_bidder` remains false when the offers source is unavailable; the model
  does not invent offer evidence.
- Public datasets can be incomplete, corrected, or published late.
- A useful ranking can still have weak precision against PACO because unusual
  procurement and prior sanctions are different concepts.
- Scores must always be reviewed with underlying evidence.

## Reproduction

See [the anomaly runbook](../runbooks/anomaly_model.md). A release run must
contain the model artifact, feature and score parquet, metrics manifest, code
commit, and promoted `current.json` pointer.
