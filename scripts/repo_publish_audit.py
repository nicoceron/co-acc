#!/usr/bin/env python3
"""Audit tracked and unignored files before publishing the repository."""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

MAX_PUBLIC_FILE_BYTES = 10 * 1024 * 1024
TEXT_SCAN_LIMIT_BYTES = 2 * 1024 * 1024

SECRET_PATTERNS = (
    re.compile(r"-----BEGIN (?:RSA |OPENSSH |EC |DSA )?PRIVATE KEY-----"),
    re.compile(r"\b(?:sk|sk-proj|sk-ant)-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{20,}\b"),
    re.compile(r"\bgh[pousr]_[0-9A-Za-z_]{20,}\b"),
)

PLACEHOLDER_WORDS = (
    "changeme",
    "change-me",
    "example",
    "placeholder",
    "replace",
    "your_",
)


@dataclass(frozen=True)
class AuditFinding:
    path: str
    reason: str


def _git_ls_files(repo_root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=repo_root,
        check=True,
        capture_output=True,
    )
    raw_paths = result.stdout.decode("utf-8").split("\0")
    return [Path(path) for path in raw_paths if path]


def _worktree_files(repo_root: Path) -> list[Path]:
    skipped_dirs = {".git", ".mypy_cache", ".pytest_cache", ".ruff_cache", ".venv", "node_modules"}
    paths: list[Path] = []
    for path in repo_root.rglob("*"):
        if any(part in skipped_dirs for part in path.parts):
            continue
        if path.is_file():
            paths.append(path.relative_to(repo_root))
    return paths


def _is_forbidden_path(path: Path) -> str | None:
    path_text = path.as_posix()
    name = path.name
    lowered_parts = [part.lower() for part in path.parts]
    if name in {"CLAUDE.md", "AGENTS.md", ".mcp.json"}:
        return "agent-private file must not be published"
    if name.startswith(".env") and name != ".env.example":
        return ".env files must not be published"
    if path.parts and path.parts[0] == "lake":
        return "runtime lake data must not be published"
    if "govt data roadmap" in "/".join(lowered_parts):
        return "private roadmap directory must not be published"
    if path_text.endswith(".pem") or path_text.endswith(".key"):
        return "private key material must not be published"
    return None


def _contains_secret(path: Path, content: str) -> bool:
    lowered = content.lower()
    if path.name == ".env.example" and any(word in lowered for word in PLACEHOLDER_WORDS):
        return False
    return any(pattern.search(content) for pattern in SECRET_PATTERNS)


def audit_paths(repo_root: Path, rel_paths: list[Path]) -> list[AuditFinding]:
    findings: list[AuditFinding] = []
    for rel_path in sorted(rel_paths, key=lambda item: item.as_posix()):
        forbidden_reason = _is_forbidden_path(rel_path)
        if forbidden_reason is not None:
            findings.append(AuditFinding(rel_path.as_posix(), forbidden_reason))
            continue

        path = repo_root / rel_path
        if not path.exists():
            continue
        try:
            size = path.stat().st_size
        except OSError as exc:
            findings.append(AuditFinding(rel_path.as_posix(), f"cannot stat file: {exc}"))
            continue

        if size > MAX_PUBLIC_FILE_BYTES:
            findings.append(AuditFinding(rel_path.as_posix(), "file exceeds 10 MB public limit"))
            continue
        if size > TEXT_SCAN_LIMIT_BYTES:
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        except OSError as exc:
            findings.append(AuditFinding(rel_path.as_posix(), f"cannot read file: {exc}"))
            continue
        if _contains_secret(rel_path, content):
            findings.append(AuditFinding(rel_path.as_posix(), "secret-like token detected"))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit files before public repo publishing")
    parser.add_argument("--repo-root", default=".", help="Repository root")
    parser.add_argument(
        "--include-worktree",
        action="store_true",
        help="Scan all non-cache worktree files, including ignored runtime files",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    rel_paths = _worktree_files(repo_root) if args.include_worktree else _git_ls_files(repo_root)
    findings = audit_paths(repo_root, rel_paths)
    if findings:
        print("FAIL")
        for finding in findings:
            print(f"- {finding.path}: {finding.reason}")
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
