#!/usr/bin/env python3
"""Start the API with Neo4j disabled and smoke-test lake-backed routes."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SECRET = "local-runtime-smoke-secret-000000000000"
DEFAULT_OFFLINE_NEO4J_URI = "bolt://127.0.0.1:9"


class SmokeError(RuntimeError):
    pass


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _json_request(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = 20.0,
) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                raise SmokeError(f"{method} {url} returned HTTP {response.status}")
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SmokeError(f"{method} {url} returned HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise SmokeError(f"{method} {url} failed: {exc.reason}") from exc

    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise SmokeError(f"{method} {url} did not return a JSON object")
    return parsed


def _wait_for_health(base_url: str, process: subprocess.Popen[str], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error = "server did not respond"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout is not None else ""
            raise SmokeError(f"API exited before health check passed:\n{output}")
        try:
            payload = _json_request(f"{base_url}/health", timeout=2.0)
        except SmokeError as exc:
            last_error = str(exc)
            time.sleep(0.5)
            continue
        if payload.get("status") == "ok":
            return
        last_error = f"unexpected health payload: {payload}"
        time.sleep(0.5)
    raise SmokeError(f"API did not become healthy within {timeout:.0f}s: {last_error}")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeError(message)


def _api_env(*, lake_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "APP_ENV": "test",
            "NEO4J_REQUIRED": "false",
            "NEO4J_URI": env.get("COACC_SMOKE_NEO4J_URI", DEFAULT_OFFLINE_NEO4J_URI),
            "COACC_LAKE_ROOT": str(lake_root),
            "JWT_SECRET_KEY": env.get("JWT_SECRET_KEY", DEFAULT_SECRET),
        }
    )
    return env


def _start_api(port: int, *, lake_root: Path) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            "uv",
            "run",
            "python",
            "-m",
            "uvicorn",
            "coacc.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=REPO_ROOT / "api",
        env=_api_env(lake_root=lake_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def run_smoke(*, lake_root: Path, timeout: float) -> dict[str, Any]:
    port = _find_free_port()
    base_url = f"http://127.0.0.1:{port}"
    process = _start_api(port, lake_root=lake_root)
    try:
        _wait_for_health(base_url, process, timeout)
        health = _json_request(f"{base_url}/health")
        _require(health.get("status") == "ok", "health status was not ok")
        _require(health.get("neo4j") == "unavailable", "smoke expected Neo4j-off mode")

        meta_health = _json_request(f"{base_url}/api/v1/meta/health")
        _require(
            meta_health.get("neo4j") == "unavailable",
            "meta health did not report Neo4j-off mode",
        )
        meta_stats = _json_request(f"{base_url}/api/v1/meta/stats")
        _require(
            int(meta_stats.get("data_sources") or 0) > 0,
            "meta stats returned no data source count",
        )

        signals = _json_request(f"{base_url}/api/v1/signals/")
        signal_items = signals.get("signals")
        _require(isinstance(signal_items, list) and len(signal_items) > 0, "no signals returned")
        assert isinstance(signal_items, list)
        _require(bool(signals.get("last_run_id")), "signals response has no last_run_id")

        cases = _json_request(f"{base_url}/api/v1/cases/?page=1&size=1")
        case_items = cases.get("cases")
        _require(isinstance(case_items, list) and len(case_items) == 1, "no case returned")
        assert isinstance(case_items, list)
        case_id = str(case_items[0].get("id") or "")
        _require(bool(case_id), "case response did not include an id")
        case_entity_ids = case_items[0].get("entity_ids")
        _require(
            isinstance(case_entity_ids, list) and bool(case_entity_ids),
            "case response did not include entity ids",
        )
        entity_id = str(case_entity_ids[0])
        encoded_entity_id = urllib.parse.quote(entity_id, safe="")

        entity = _json_request(f"{base_url}/api/v1/entity/{encoded_entity_id}")
        _require(
            bool(entity.get("id") or entity.get("properties")),
            "entity lookup returned no entity",
        )
        search = _json_request(
            f"{base_url}/api/v1/search?q={encoded_entity_id}&page=1&size=1"
        )
        _require(int(search.get("total") or 0) > 0, "search returned no lake entity matches")
        entity_signals = _json_request(
            f"{base_url}/api/v1/entity/{encoded_entity_id}/signals"
        )
        _require(
            int(entity_signals.get("total") or 0) > 0,
            "entity signal route returned no lake signals",
        )
        patterns = _json_request(f"{base_url}/api/v1/patterns/{encoded_entity_id}")
        _require(
            int(patterns.get("total") or 0) > 0,
            "pattern route returned no lake-backed patterns",
        )
        evidence_trail = _json_request(
            f"{base_url}/api/v1/entity/{encoded_entity_id}/evidence-trail"
        )
        _require(
            int(evidence_trail.get("total_bundles") or 0) > 0,
            "evidence trail route returned no lake-backed bundles",
        )
        exposure = _json_request(f"{base_url}/api/v1/entity/{encoded_entity_id}/exposure")
        _require(
            float(exposure.get("exposure_index") or 0.0) > 0.0,
            "exposure route returned no lake-backed exposure",
        )
        timeline = _json_request(f"{base_url}/api/v1/entity/{encoded_entity_id}/timeline")
        _require(
            int(timeline.get("total") or 0) > 0,
            "timeline route returned no lake-backed events",
        )
        graph = _json_request(f"{base_url}/api/v1/graph/{encoded_entity_id}?depth=1")
        _require(
            bool(graph.get("nodes")) and bool(graph.get("edges")),
            "graph route returned no lake-backed graph",
        )
        baseline = _json_request(f"{base_url}/api/v1/baseline/{encoded_entity_id}")
        _require(
            int(baseline.get("total") or 0) > 0,
            "baseline route returned no lake-backed peer comparisons",
        )

        case_detail = _json_request(f"{base_url}/api/v1/cases/{case_id}")
        _require(case_detail.get("id") == case_id, "case detail id mismatch")
        _require(
            bool(case_detail.get("signals")) or bool(case_detail.get("anomaly_score")),
            "case detail has neither signals nor anomaly_score",
        )

        agent = _json_request(
            f"{base_url}/api/v1/agent/query",
            method="POST",
            payload={"question": "Resume este caso con citas", "case_id": case_id},
        )
        citations = agent.get("citations")
        _require(isinstance(citations, list) and len(citations) > 0, "agent returned no citations")
        assert isinstance(citations, list)
        _require(bool(agent.get("answer")), "agent returned no answer")

        return {
            "base_url": base_url,
            "signal_count": len(signal_items),
            "last_signal_run_id": signals.get("last_run_id"),
            "case_id": case_id,
            "entity_id": entity_id,
            "agent_citation_count": len(citations),
            "data_sources": meta_stats.get("data_sources"),
        }
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lake-root",
        type=Path,
        default=REPO_ROOT / "lake",
        help="Lake root used by the API. Defaults to ./lake.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Seconds to wait for API startup.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = run_smoke(lake_root=args.lake_root.resolve(), timeout=args.timeout)
    except SmokeError as exc:
        print(f"FAIL {exc}", file=sys.stderr)
        return 1
    print(
        "PASS lake-backed API smoke: "
        f"{result['signal_count']} signal(s), "
        f"case={result['case_id']}, "
        f"entity={result['entity_id']}, "
        f"citations={result['agent_citation_count']}, "
        f"sources={result['data_sources']}, "
        f"run={result['last_signal_run_id']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
