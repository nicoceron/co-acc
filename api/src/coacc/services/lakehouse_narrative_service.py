from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime

from coacc.services import lakehouse_query

_SAFE_CASE_ID = re.compile(r"[^A-Za-z0-9_.=-]+")


@dataclass(frozen=True)
class LakeNarrative:
    markdown: str
    generated_at: str
    path: str


def _safe_case_id(case_id: str) -> str:
    return _SAFE_CASE_ID.sub("_", case_id.strip()).strip("._") or "case"


def read_lake_narrative(case_id: str, *, aliases: list[str] | None = None) -> LakeNarrative | None:
    candidates = list(dict.fromkeys([case_id, *(aliases or [])]))
    root = lakehouse_query.lake_root() / "curated" / "narratives"
    for candidate in candidates:
        path = root / f"{_safe_case_id(candidate)}.md"
        if not path.exists() or not path.is_file():
            continue
        try:
            markdown = path.read_text(encoding="utf-8")
        except OSError:
            continue
        generated_at = datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat()
        return LakeNarrative(markdown=markdown, generated_at=generated_at, path=str(path))
    return None
