# Generative Narrator Card

The narrator turns a lake-backed case subgraph and anomaly score into a
Spanish, citation-bound Markdown narrative for reviewer workflows.

- Inputs: contract-level anomaly score parquet, curated contract award rows,
  materialized signal hits, and evidence bundles.
- Runtime: batch ETL CLI, not request path.
- Output: `lake/curated/narratives/<case_id>.md`.
- Provider path: Gemini, Anthropic, or OpenAI when a key is present; otherwise
  the documented templated fallback is used for local reproducibility.

The verifier requires `# Lead`, `## Evidencia`, `## Señales`, and
`## Fuentes`; 250-400 words; only known entities; resolvable citations; and no
criminality or culpability language.
