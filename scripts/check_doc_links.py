#!/usr/bin/env python3
"""Check local Markdown links in the CRISP-ML documentation."""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

DEFAULT_DOCS = ("docs/crisp_ml.md",)
INLINE_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")


@dataclass(frozen=True)
class LinkFinding:
    path: str
    target: str
    reason: str


def _git_markdown_files(repo_root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "*.md"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        encoding="utf-8",
    )
    return [Path(line) for line in result.stdout.splitlines() if line.strip()]


def _is_external(target: str) -> bool:
    parsed = urlparse(target)
    return bool(parsed.scheme and parsed.scheme not in {"", "file"})


def _target_path(source: Path, target: str) -> Path | None:
    clean = unquote(target.split("#", 1)[0]).strip()
    if not clean or clean.startswith("#") or _is_external(clean):
        return None
    if clean.startswith("mailto:"):
        return None
    return (source.parent / clean).resolve()


def check_paths(repo_root: Path, rel_paths: list[Path]) -> list[LinkFinding]:
    findings: list[LinkFinding] = []
    for rel_path in rel_paths:
        path = repo_root / rel_path
        if not path.exists():
            findings.append(LinkFinding(rel_path.as_posix(), "", "document does not exist"))
            continue
        content = path.read_text(encoding="utf-8")
        for match in INLINE_LINK.finditer(content):
            raw_target = match.group(1).strip()
            target = _target_path(path, raw_target)
            if target is None:
                continue
            try:
                target.relative_to(repo_root)
            except ValueError:
                findings.append(
                    LinkFinding(rel_path.as_posix(), raw_target, "target escapes repo")
                )
                continue
            if not target.exists():
                findings.append(LinkFinding(rel_path.as_posix(), raw_target, "missing target"))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Check local Markdown links")
    parser.add_argument("--repo-root", default=".", help="Repository root")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Scan all tracked Markdown files instead of docs/crisp_ml.md",
    )
    parser.add_argument("paths", nargs="*", help="Specific Markdown files to scan")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    rel_paths = (
        [Path(path) for path in args.paths]
        if args.paths
        else (_git_markdown_files(repo_root) if args.all else [Path(path) for path in DEFAULT_DOCS])
    )
    findings = check_paths(repo_root, rel_paths)
    if findings:
        print("FAIL")
        for finding in findings:
            print(f"- {finding.path}: {finding.target}: {finding.reason}")
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
