# Confidence index

Every materialized signal is visible. `confidence_index` reports how well the
underlying match is supported; it does **not** estimate the probability of
corruption and it does not replace the separate pattern `score`.

The index is a 0–100 weighted score:

```text
confidence_index = 100 × (
  0.55 × identity_match
  + 0.25 × evidence_traceability
  + 0.20 × source_corroboration
)
```

Components:

- **Identity match (55%)**: exact identifier `1.00`, high-quality match `0.90`,
  probable match `0.75`, and unknown match `0.50`.
- **Evidence traceability (25%)**: no linked evidence `0.00`, one item `0.65`,
  two items `0.85`, and three or more items `1.00`.
- **Source corroboration (20%)**: no attributable source `0.00`, one source
  `0.70`, two sources `0.90`, and three or more sources `1.00`.

The API returns both the index and its three components for every hit. Pattern
and signal catalog rows show the average confidence of their materialized hits.
Registered definitions without hits display no confidence value rather than an
invented score.

The existing `score` remains the detector-specific strength or anomaly score.
A high confidence index means the match is well supported, not that wrongdoing
has been established.
