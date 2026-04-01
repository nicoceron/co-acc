from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_IDENTITY_ALIAS_STATEMENTS = [
    """
    MATCH (c:Company)
    WHERE coalesce(c.document_id, '') <> ''
    MERGE (a:Alias {alias_id: 'company:document:' + c.document_id})
    SET a.kind = 'company_document',
        a.value = c.document_id,
        a.normalized = c.document_id,
        a.source_id = coalesce(c.source, 'graph'),
        a.confidence = 1.0
    MERGE (a)-[:ALIAS_OF {match_type: 'EXACT_COMPANY_NIT', confidence: 1.0}]->(c)
    """,
    """
    MATCH (c:Company)
    WHERE coalesce(c.nit, '') <> ''
    MERGE (a:Alias {alias_id: 'company:nit:' + c.nit})
    SET a.kind = 'company_nit',
        a.value = c.nit,
        a.normalized = c.nit,
        a.source_id = coalesce(c.source, 'graph'),
        a.confidence = 1.0
    MERGE (a)-[:ALIAS_OF {match_type: 'EXACT_COMPANY_NIT', confidence: 1.0}]->(c)
    """,
    """
    MATCH (p:Person)
    WHERE coalesce(p.document_id, '') <> ''
    MERGE (a:Alias {alias_id: 'person:document:' + p.document_id})
    SET a.kind = 'person_document',
        a.value = p.document_id,
        a.normalized = p.document_id,
        a.source_id = coalesce(p.source, 'graph'),
        a.confidence = 1.0
    MERGE (a)-[:ALIAS_OF {match_type: 'EXACT_PERSON_DOCUMENT', confidence: 1.0}]->(p)
    """,
    """
    MATCH (ct:Contract)
    WHERE coalesce(ct.contract_id, '') <> ''
    MERGE (a:Alias {alias_id: 'contract:key:' + ct.contract_id})
    SET a.kind = 'contract_key',
        a.value = ct.contract_id,
        a.normalized = ct.contract_id,
        a.source_id = coalesce(ct.source, 'graph'),
        a.confidence = 1.0
    MERGE (a)-[:ALIAS_OF {match_type: 'EXACT_CONTRACT_KEY', confidence: 1.0}]->(ct)
    """,
    """
    MATCH (pr:Project)
    WHERE coalesce(pr.project_id, '') <> ''
    MERGE (a:Alias {alias_id: 'project:bpin:' + pr.project_id})
    SET a.kind = 'project_bpin',
        a.value = pr.project_id,
        a.normalized = pr.project_id,
        a.source_id = coalesce(pr.source, 'graph'),
        a.confidence = 1.0
    MERGE (a)-[:ALIAS_OF {match_type: 'EXACT_BPIN', confidence: 1.0}]->(pr)
    """,
]


def _split_statements(raw: str) -> list[str]:
    statements = [s.strip() for s in raw.split(";") if s.strip()]
    cleaned: list[str] = []
    for stmt in statements:
        lines = [ln for ln in stmt.splitlines() if not ln.strip().startswith("//")]
        cypher = "\n".join(lines).strip()
        if cypher:
            cleaned.append(cypher)
    return cleaned


def _run_script(driver: Driver, neo4j_database: str, script_path: Path) -> None:
    raw = script_path.read_text(encoding="utf-8")
    statements = _split_statements(raw)
    if not statements:
        return
    with driver.session(database=neo4j_database) as session:
        for stmt in statements:
            session.run(stmt)
    logger.info(
        "Post-load linking script applied: %s (%d statements)",
        script_path.name,
        len(statements),
    )


def run_post_load_hooks(
    *,
    driver: Driver,
    source: str,
    neo4j_database: str,
    linking_tier: str,
) -> None:
    tier = linking_tier.strip().lower()
    if tier not in {"community", "full"}:
        tier = "full"

    if tier == "community":
        logger.info("Post-load hooks skipped (LINKING_TIER=community)")
        return

    repo_root = Path(__file__).resolve().parents[3]
    scripts_dir = repo_root / "scripts"

    # CO-ACC Colombia currently handles entity resolution within the pipelines
    # or via dedicated Colombia-scoped linking scripts (TBD).
    script_names: list[str] = []

    if not script_names:
        logger.info("No file-based post-load linking hook configured for source=%s", source)

    for name in script_names:
        script_path = scripts_dir / name
        if not script_path.exists():
            logger.warning("Post-load linking script missing (skipped): %s", script_path)
            continue
        _run_script(driver, neo4j_database, script_path)

    with driver.session(database=neo4j_database) as session:
        for stmt in _IDENTITY_ALIAS_STATEMENTS:
            session.run(stmt)
    logger.info("Identity alias hooks applied for source=%s", source)
