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
- Providers: Gemini, Anthropic, or OpenAI when a key is configured.
- Local fallback: deterministic templated narrative when no provider key is
  available and `--require-llm` is not set.

This is a Phase 14 foundation slice. It adds extraction, prompt construction,
provider call wiring, verifier checks, fallback generation, CLI, and tests. It
does not yet close the full Phase 14 fixture requirement: recorded LLM
responses for 10 fixture subgraphs remain open.

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
