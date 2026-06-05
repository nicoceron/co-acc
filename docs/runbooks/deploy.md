# Production Deploy Runbook

Production deploys should prove readiness, not only liveness. The deploy
script starts services and then runs the production smoke against the public
URL.

## Deploy

On the production host:

```bash
DOMAIN=coacc.example.com \
JWT_SECRET_KEY=replace-with-prod-secret \
NEO4J_PASSWORD=replace-with-prod-secret \
infra/scripts/deploy.sh
```

The script checks:

- `/ready`
- `/health`
- `/api/v1/meta/operations`
- `/api/v1/meta/stats`
- `/api/v1/public/meta`
- `/api/v1/signals/`
- one lake-backed case, entity, entity-signal, and case-detail path
- frontend root HTML

## Manual Smoke

```bash
python3 scripts/production_smoke.py --base-url https://$DOMAIN
```

For local or self-signed TLS targets:

```bash
python3 scripts/production_smoke.py --base-url https://$DOMAIN --insecure
```

For API-only smoke when the frontend is not routed yet:

```bash
python3 scripts/production_smoke.py --base-url http://localhost:8000 --skip-frontend
```

## Controls

| Variable | Default | Purpose |
|---|---:|---|
| `COACC_PROD_SMOKE_BASE_URL` | `https://$DOMAIN` | Override smoke target URL. |
| `COACC_PROD_SMOKE_WAIT_TIMEOUT` | `120` | Seconds to wait for `/ready`. |
| `COACC_PROD_SMOKE_INSECURE` | `true` in deploy script | Skip TLS verification for first-run/self-signed deploys. |
| `COACC_PROD_SMOKE_SKIP` | `false` | Skip production smoke and fall back to `/health` only. Use only during emergency recovery. |
| `COACC_PROD_SMOKE_SKIP_FRONTEND` | `false` | Skip root HTML check. |
| `COACC_PROD_SMOKE_REQUIRE_OPS` | `true` | Require `/api/v1/meta/operations` to be healthy. |
| `COACC_PROD_SMOKE_REQUIRE_MATERIALIZED_ONLY` | `true` | Require the signal catalog to expose only materialized signals. |

## Failure

If smoke fails, `deploy.sh` prints the last 50 compose log lines and exits
nonzero. The most common failure modes are:

- `/ready` fails because the durable lake or config mounts are missing.
- `lake/meta/operations/latest.json` is missing, failed, or stale.
- `COACC_SIGNALS_REQUIRE_MATERIALIZED=true` is not set while registered-only
  signals are exposed.
- The frontend proxy route is not serving `/`.
