# Generative Narrator Runbook

The Phase 14 narrator slice precomputes verified Markdown narratives from the
local parquet lake. It does not require Neo4j.

## Generate A Narrative

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl narrator generate <case-id>
```

`<case-id>` may be a contract id, a lake signal hit id that resolves to a
contract, or an API anomaly case id. The command writes:

```text
lake/curated/narratives/<case-id>.md
```

The lake-backed case API reads the same file on detail responses:

```bash
curl http://localhost:8000/api/v1/cases/<case-id>
```

When present, the response includes `narrative_markdown` and
`narrative_generated_at`. If a narrative was generated for the underlying
contract id, anomaly case detail responses can use it as a fallback.

By default the provider is `gemini`. If no provider key is present, the command
uses the verified templated fallback so local and CI runs remain reproducible.

## Force A Live Provider

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl narrator generate <case-id> \
  --provider gemini \
  --require-llm
```

Supported providers are `gemini`, `anthropic`, `openai`, and `template`.
Environment variables:

- `GEMINI_API_KEY`, `GOOGLE_API_KEY`, or `GOOGLE_GENERATIVE_AI_API_KEY`
- `ANTHROPIC_API_KEY`
- `OPENAI_API_KEY`

Use `--model` to override the provider default and `--output` to write to a
specific Markdown path.

## Batch Precompute

Generate narratives for the highest-scoring promoted anomaly cases:

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl narrator generate-batch \
  --limit 25 \
  --min-score 0.7
```

The batch command reads `lake/models/anomaly/current.json`, orders the active
`lake/curated/anomaly_scores/run_id=<run>/` partition by score, and writes one
Markdown file per selected contract id. It skips existing files by default; use
`--overwrite` to regenerate.

## Verification

Every generated or templated narrative goes through
`coacc_etl.models.narrator.verify.check`. The verifier enforces required
sections, 250-400 words, known entity mentions, resolvable citations, signed
dataset ids, and the ethics guard against culpability language.

## Current Scope

This slice proves the narrator runtime path and safety checks. Remaining Phase
14 work:

- recorded LLM fixture responses for 10 subgraphs;
- frontend narrative display;
- richer subgraph extraction beyond contract, buyer, supplier, signals, and
  evidence rows.
