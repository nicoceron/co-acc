#!/usr/bin/env python3
"""Smoke-test a production-style CO-ACC deployment by URL."""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


class SmokeError(RuntimeError):
    pass


def _json_request(
    url: str,
    *,
    timeout: float,
    context: ssl.SSLContext | None,
) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            body = response.read().decode("utf-8")
            status = response.status
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SmokeError(f"GET {url} returned HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise SmokeError(f"GET {url} failed: {exc.reason}") from exc
    if status != 200:
        raise SmokeError(f"GET {url} returned HTTP {status}")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SmokeError(f"GET {url} did not return JSON") from exc
    if not isinstance(payload, dict):
        raise SmokeError(f"GET {url} did not return a JSON object")
    return payload


def _text_request(
    url: str,
    *,
    timeout: float,
    context: ssl.SSLContext | None,
) -> str:
    request = urllib.request.Request(url, headers={"Accept": "text/html,*/*"})
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            body = response.read().decode("utf-8", errors="replace")
            status = response.status
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SmokeError(f"GET {url} returned HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise SmokeError(f"GET {url} failed: {exc.reason}") from exc
    if status != 200:
        raise SmokeError(f"GET {url} returned HTTP {status}")
    return body


def _url(base_url: str, path: str) -> str:
    return urllib.parse.urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeError(message)


def _wait_for_ready(
    base_url: str,
    *,
    timeout: float,
    request_timeout: float,
    context: ssl.SSLContext | None,
) -> None:
    deadline = time.monotonic() + timeout
    last_error = "deployment did not respond"
    while time.monotonic() < deadline:
        try:
            payload = _json_request(
                _url(base_url, "/ready"),
                timeout=request_timeout,
                context=context,
            )
        except SmokeError as exc:
            last_error = str(exc)
            time.sleep(1.0)
            continue
        if payload.get("status") == "ready":
            return
        last_error = f"unexpected readiness payload: {payload}"
        time.sleep(1.0)
    raise SmokeError(f"/ready did not pass within {timeout:.0f}s: {last_error}")


def _case_and_entity_checks(
    base_url: str,
    *,
    timeout: float,
    context: ssl.SSLContext | None,
) -> tuple[str, str]:
    cases = _json_request(_url(base_url, "/api/v1/cases/?page=1&size=1"), timeout=timeout, context=context)
    case_items = cases.get("cases")
    _require(isinstance(case_items, list) and len(case_items) == 1, "no lake-backed case returned")
    assert isinstance(case_items, list)
    case_id = str(case_items[0].get("id") or "")
    _require(bool(case_id), "case response did not include an id")
    entity_ids = case_items[0].get("entity_ids")
    _require(isinstance(entity_ids, list) and bool(entity_ids), "case response has no entity ids")
    assert isinstance(entity_ids, list)
    entity_id = str(entity_ids[0])
    encoded = urllib.parse.quote(entity_id, safe="")

    entity = _json_request(_url(base_url, f"/api/v1/entity/{encoded}"), timeout=timeout, context=context)
    _require(bool(entity.get("id") or entity.get("properties")), "entity lookup returned no entity")

    entity_signals = _json_request(
        _url(base_url, f"/api/v1/entity/{encoded}/signals"),
        timeout=timeout,
        context=context,
    )
    _require(int(entity_signals.get("total") or 0) > 0, "entity route returned no signals")

    case_detail = _json_request(_url(base_url, f"/api/v1/cases/{case_id}"), timeout=timeout, context=context)
    _require(case_detail.get("id") == case_id, "case detail id mismatch")
    _require(bool(case_detail.get("signals")), "case detail returned no materialized signals")
    return case_id, entity_id


def run_smoke(
    *,
    base_url: str,
    timeout: float,
    wait_timeout: float,
    insecure: bool,
    skip_frontend: bool,
    require_materialized_only: bool,
    require_ops: bool,
) -> dict[str, Any]:
    context = ssl._create_unverified_context() if insecure else None  # noqa: S323
    _wait_for_ready(
        base_url,
        timeout=wait_timeout,
        request_timeout=timeout,
        context=context,
    )

    health = _json_request(_url(base_url, "/health"), timeout=timeout, context=context)
    _require(health.get("status") == "ok", "/health status was not ok")

    ready = _json_request(_url(base_url, "/ready"), timeout=timeout, context=context)
    _require(ready.get("status") == "ready", "/ready status was not ready")

    operations = _json_request(_url(base_url, "/api/v1/meta/operations"), timeout=timeout, context=context)
    if require_ops:
        _require(operations.get("healthy") is True, "lake operations status is not healthy")
        _require(operations.get("status") == "succeeded", "latest lake ops did not succeed")

    stats = _json_request(_url(base_url, "/api/v1/meta/stats"), timeout=timeout, context=context)
    _require(int(stats.get("data_sources") or 0) > 0, "meta stats has no data sources")
    _require(int(stats.get("loaded_sources") or 0) > 0, "meta stats has no loaded sources")

    public_meta = _json_request(_url(base_url, "/api/v1/public/meta"), timeout=timeout, context=context)
    _require(public_meta.get("mode") == "public_safe", "public meta mode is not public_safe")

    signals = _json_request(_url(base_url, "/api/v1/signals/"), timeout=timeout, context=context)
    signal_items = signals.get("signals")
    _require(isinstance(signal_items, list) and len(signal_items) > 0, "no signals returned")
    assert isinstance(signal_items, list)
    _require(bool(signals.get("last_run_id")), "signals response has no last_run_id")
    if require_materialized_only:
        registered_only = [
            item.get("id")
            for item in signal_items
            if item.get("materialization_state") != "materialized"
        ]
        _require(
            not registered_only,
            "production signal list includes registered-only signals: "
            + ", ".join(str(item) for item in registered_only[:5]),
        )

    case_id, entity_id = _case_and_entity_checks(base_url, timeout=timeout, context=context)

    if not skip_frontend:
        html = _text_request(_url(base_url, "/"), timeout=timeout, context=context)
        _require("<html" in html.lower(), "frontend root did not return HTML")
        _require("/assets/" in html or "type=\"module\"" in html, "frontend shell has no asset entry")

    return {
        "base_url": base_url,
        "signal_count": len(signal_items),
        "last_signal_run_id": signals.get("last_run_id"),
        "case_id": case_id,
        "entity_id": entity_id,
        "loaded_sources": stats.get("loaded_sources"),
        "lake_ops_run_id": operations.get("run_id"),
    }


def _default_base_url() -> str:
    configured = os.environ.get("COACC_PROD_SMOKE_BASE_URL", "").strip()
    if configured:
        return configured
    domain = os.environ.get("DOMAIN", "").strip()
    if domain:
        return f"https://{domain}"
    return "http://localhost"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=_default_base_url())
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--wait-timeout", type=float, default=120.0)
    parser.add_argument("--insecure", action="store_true", help="Skip TLS verification.")
    parser.add_argument("--skip-frontend", action="store_true")
    parser.add_argument(
        "--no-require-materialized-only",
        action="store_true",
        help="Do not require every listed signal to be materialized.",
    )
    parser.add_argument(
        "--no-require-ops",
        action="store_true",
        help="Do not require /api/v1/meta/operations to be healthy.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = run_smoke(
            base_url=args.base_url,
            timeout=args.timeout,
            wait_timeout=args.wait_timeout,
            insecure=args.insecure,
            skip_frontend=args.skip_frontend,
            require_materialized_only=not args.no_require_materialized_only,
            require_ops=not args.no_require_ops,
        )
    except SmokeError as exc:
        print(f"FAIL production smoke: {exc}", file=sys.stderr)
        return 1

    print(
        "PASS production smoke: "
        f"{result['signal_count']} signal(s), "
        f"case={result['case_id']}, "
        f"entity={result['entity_id']}, "
        f"sources={result['loaded_sources']}, "
        f"signal_run={result['last_signal_run_id']}, "
        f"ops_run={result['lake_ops_run_id']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
