# CO-ACC Generative Narrator Model Card

## Purpose

The narrator produces a Spanish Markdown case narrative from a lake-backed
subgraph and anomaly score. It is designed for reviewer triage, not for legal
findings. The verifier rejects criminality or culpability language and requires
all factual claims to cite evidence rows from the case subgraph.

## Current Model

- Runtime: batch ETL CLI, not request path.
- Input: anomaly score parquet, curated SECOP contract awards, materialized
  signal hits, and evidence bundles.
- Output: `lake/curated/narratives/<case_id>.md`.
- API surface: `GET /api/v1/cases/{case_id}` returns `narrative_markdown`
  and `narrative_generated_at` when a precomputed narrative exists.
- Providers: Gemini, Anthropic, or OpenAI when a key is configured.
- Local fallback: deterministic templated narrative when no provider key is
  available and `--require-llm` is not set.
- Batch path: `coacc-etl narrator generate-batch --limit N --min-score X`
  precomputes top promoted anomaly cases and skips existing Markdown by default.

This is a Phase 14 backend slice. It adds extraction, prompt construction,
provider call wiring, verifier checks, fallback generation, CLI, batch
precompute, API serving support, and recorded no-network fixture coverage for
10 subgraphs. Frontend display remains outside this backend slice.

## Verifier Contract

A generated narrative is accepted only when it:

- includes `# Lead`, `## Evidencia`, `## Señales`, and `## Fuentes`;
- is 250-400 words;
- mentions only entities present in the extracted subgraph;
- uses citations of the form `(dataset_id, row_key)`;
- cites only dataset ids from the signed catalog or ETL dataset contracts;
- cites only row keys present in the case evidence list;
- avoids forbidden criminality or culpability terms.

## Operational Notes

Use `docs/runbooks/narrator.md` for commands. In local development, the default
provider is `gemini`; without `GEMINI_API_KEY`, the command writes the verified
template fallback. Use `--require-llm` in operator runs that must prove a live
provider path.
