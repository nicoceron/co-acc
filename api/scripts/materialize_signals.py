#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from coacc.dependencies import close_driver, init_driver  # noqa: E402
from coacc.routers import meta as meta_router  # noqa: E402
from coacc.services import investigation_service  # noqa: E402
from coacc.services.case_service import refresh_case  # noqa: E402
from coacc.services.intelligence_provider import get_default_provider  # noqa: E402
from coacc.services.neo4j_service import execute_query  # noqa: E402
from coacc.services.signal_materializer import refresh_entity_signals  # noqa: E402


def _read_entity_file(path: str | None) -> list[str]:
    if not path:
        return []
    rows = Path(path).read_text(encoding="utf-8").splitlines()
    return [row.strip() for row in rows if row.strip() and not row.strip().startswith("#")]


async def _resolve_watchlist_entities(
    session,
    watchlist: str | None,
    limit: int,
) -> list[str]:
    if watchlist == "people":
        response = await meta_router.prioritized_people_watchlist(session=session, limit=limit)
        return [item.entity_id for item in response.people]
    if watchlist == "companies":
        response = await meta_router.prioritized_company_watchlist(session=session, limit=limit)
        return [item.entity_id for item in response.companies]
    if watchlist == "buyers":
        response = await meta_router.prioritized_buyer_watchlist(session=session, limit=limit)
        return [item.buyer_id for item in response.buyers]
    if watchlist == "territories":
        response = await meta_router.prioritized_territory_watchlist(session=session, limit=limit)
        return [item.territory_id for item in response.territories]
    return []


async def _resolve_entities(
    session,
    investigation_id: str | None,
    user_id: str | None,
    entities: list[str],
    entity_file: str | None,
    watchlist: str | None,
    limit: int,
) -> list[str]:
    collected = [*entities, *_read_entity_file(entity_file)]
    if investigation_id:
        if not user_id:
            msg = "--user-id is required when using --investigation-id"
            raise SystemExit(msg)
        investigation = await investigation_service.get_investigation(
            session,
            investigation_id,
            user_id,
        )
        if investigation is None:
            msg = f"Investigation not found: {investigation_id}"
            raise SystemExit(msg)
        collected.extend(investigation.entity_ids)

    collected.extend(await _resolve_watchlist_entities(session, watchlist, limit))
    return list(dict.fromkeys(collected))


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Materialize CO-ACC signal hits for entities or an investigation case.",
    )
    parser.add_argument("entities", nargs="*", help="Entity identifiers or element ids")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Materialize signals for all candidate Company/Person/Project/Contract entities",
    )
    parser.add_argument(
        "--case-id",
        dest="case_id",
        help="Persist and refresh a Case/Investigation dossier by id",
    )
    parser.add_argument(
        "--investigation-id",
        dest="legacy_investigation_id",
        help="Deprecated alias for --case-id",
    )
    parser.add_argument(
        "--user-id",
        dest="user_id",
        help="Owner user id for investigation lookup",
    )
    parser.add_argument(
        "--entity-file",
        dest="entity_file",
        help="Path to a newline-delimited entity list",
    )
    parser.add_argument(
        "--watchlist",
        choices=("people", "companies", "buyers", "territories"),
        help="Expand entities from a prioritized watchlist before materialization",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum watchlist entities to expand",
    )
    parser.add_argument("--lang", default="es", help="Signal language (default: es)")
    args = parser.parse_args()

    driver = await init_driver()
    provider = get_default_provider()
    try:
        summaries: list[dict[str, object]] = []
        async with driver.session() as session:
            case_id = args.case_id or args.legacy_investigation_id
            if case_id:
                if not args.user_id:
                    raise SystemExit("--user-id is required when using --case-id")
                case_response = await refresh_case(
                    session,
                    case_id,
                    args.user_id,
                    provider,
                    lang=args.lang,
                )
                if case_response is None:
                    raise SystemExit(f"Case not found: {case_id}")
                print(
                    json.dumps(
                        {
                            "case": {
                                "id": case_response.id,
                                "signal_count": case_response.signal_count,
                                "public_signal_count": case_response.public_signal_count,
                                "last_run_id": case_response.last_run_id,
                                "last_refreshed_at": case_response.last_refreshed_at,
                                "stale": case_response.stale,
                                "entity_ids": case_response.entity_ids,
                            },
                        },
                        ensure_ascii=True,
                        indent=2,
                    )
                )
                return

            entity_ids = await _resolve_entities(
                session,
                None,
                args.user_id,
                args.entities,
                args.entity_file,
                args.watchlist,
                max(1, args.limit),
            )
            if args.all:
                candidate_limit = max(1000, max(1, args.limit))
                candidate_records = await execute_query(
                    session,
                    "signal_entity_candidates",
                    {"limit": candidate_limit},
                )
                entity_ids.extend(str(record["entity_id"]) for record in candidate_records)
                entity_ids = list(dict.fromkeys(entity_ids))
            if not entity_ids:
                raise SystemExit(
                    "Provide entity ids, --entity-file, --watchlist, --all, or --case-id"
                )
            for entity_id in entity_ids:
                response = await refresh_entity_signals(
                    session,
                    entity_id,
                    provider,
                    lang=args.lang,
                    trigger="nightly" if args.all else "manual",
                    created_by=args.user_id,
                )
                summaries.append(
                    {
                        "entity_id": response.entity_id,
                        "entity_key": response.entity_key,
                        "signal_total": response.total,
                        "last_run_id": response.last_run_id,
                        "last_refreshed_at": response.last_refreshed_at,
                        "stale": response.stale,
                        "signals": [signal.signal_id for signal in response.signals],
                    }
                )
        payload: dict[str, object] = {"entities": summaries}
        if args.all:
            payload["mode"] = "global"
            payload["entity_count"] = len(summaries)
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    finally:
        await close_driver()


if __name__ == "__main__":
    asyncio.run(_main())
