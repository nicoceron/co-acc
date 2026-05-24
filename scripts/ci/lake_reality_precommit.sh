#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cd "$ROOT"
if ! git diff --cached --name-only -- "etl/datasets/*.yml" | grep -q .; then
  exit 0
fi
COACC_REALITY_STAGED_ONLY=1 make lake-reality CHANGED_YAMLS_ONLY=1
