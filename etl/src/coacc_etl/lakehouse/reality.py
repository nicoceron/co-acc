from __future__ import annotations

import argparse
import logging
import os
import sys

from coacc_etl.streaming import _socrata_request

logger = logging.getLogger(__name__)


def socrata_live_count(
    dataset_id: str,
    *,
    where: str | None = None,
    domain: str | None = None,
) -> int:
    params: dict[str, str | int] = {"$select": "count(*)"}
    if where:
        params["$where"] = where
    rows = _socrata_request(
        dataset_id,
        params=params,
        domain=domain,
        timeout=120.0,
        max_attempts=10,
        log_label="live_count",
    )
    if not rows:
        return 0

    count_str = ""
    for row in rows:
        candidate = row.get("count") or row.get("COUNT") or row.get("count_*")
        if candidate is not None:
            count_str = str(candidate)
            break

    if not count_str:
        all_keys: set[str] = set()
        for row in rows:
            all_keys.update(row.keys())
        raise ValueError(
            f"socrata_live_count: no count column in response for {dataset_id}. "
            f"Keys: {sorted(all_keys)}"
        )

    return int(count_str.replace(",", ""))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print the live row count of a Socrata dataset."
    )
    parser.add_argument("dataset_id", help="Socrata dataset ID (e.g. jbjy-vk9h)")
    parser.add_argument("--where", default=None, help="Optional $where clause")
    parser.add_argument("--domain", default=None, help="Socrata domain (default: SOCRATA_DOMAIN env)")
    args = parser.parse_args()

    count = socrata_live_count(args.dataset_id, where=args.where, domain=args.domain)
    print(f"{args.dataset_id}: {count:,}")
    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    main()