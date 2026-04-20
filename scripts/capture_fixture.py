#!/usr/bin/env python3
"""Capture a live sample from a Socrata dataset and write it as a JSON fixture.

Usage:
    python scripts/capture_fixture.py jbjy-vk9h --limit 200 \
        --out etl/tests/fixtures/secop_integrado/jbjy_vk9h_sample.json
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

from coacc_etl.streaming import socrata_get

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture live Socrata sample as JSON fixture")
    parser.add_argument("dataset_id", help="Socrata dataset ID (e.g. jbjy-vk9h)")
    parser.add_argument("--limit", type=int, default=200, help="Number of rows to pull")
    parser.add_argument("--out", required=True, help="Output JSON path")
    parser.add_argument("--domain", default=None, help="Socrata domain override")
    args = parser.parse_args()

    rows = socrata_get(
        args.dataset_id,
        limit=args.limit,
        offset=0,
        order=":id",
        domain=args.domain,
        timeout=120.0,
        max_attempts=10,
    )

    if not rows:
        print(f"ERROR: no rows returned from {args.dataset_id}", file=sys.stderr)
        sys.exit(1)

    from pathlib import Path

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    main()