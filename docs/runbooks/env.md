# Environment Runbook

Use `.env.example` as the tracked template. `make setup-env` creates a local
`.env` and replaces the placeholder `JWT_SECRET_KEY` and `NEO4J_PASSWORD`.
Never commit `.env`.

## Lake / ETL

| Variable | Default | Purpose |
|---|---:|---|
| `COACC_LAKE_ROOT` | repo `lake/` via Make targets | Lake root for raw, curated, and metadata parquet. |
| `COACC_SOCRATA_PAGE_SIZE` | `10000` | Rows requested per Socrata page when CLI flags do not override it. |
| `COACC_SOCRATA_MAX_PAGES` | `10000` | Hard page cap per Socrata dataset when CLI flags do not override it. |
| `COACC_WATERMARK_FUTURE_GRACE_DAYS` | `1` | Maximum allowed timestamp lead before a row is treated as implausibly future-dated and routed to `year=0/month=00` without advancing the watermark. |

## External APIs

| Variable | Purpose |
|---|---|
| `SOCRATA_APP_TOKEN` | Optional Socrata app token for higher-rate datos.gov.co requests. |
| `SOCRATA_KEY_ID` / `SOCRATA_KEY_SECRET` | Optional Socrata credentials for authenticated requests. |
| `GEMINI_API_KEY` | Optional Gemini key for source qualification LLM review. |
| `ANTHROPIC_API_KEY` | Optional Anthropic key for source qualification LLM review. |
| `OPENAI_API_KEY` | Optional OpenAI key for source qualification LLM review. |

## App Runtime

The API, frontend, and Neo4j variables are listed in `.env.example`. Keep
production secrets out of this repository and rotate any value that is ever
printed or committed accidentally.
