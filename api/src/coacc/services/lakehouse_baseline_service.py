from __future__ import annotations

import re
from typing import Any

from coacc.models.baseline import BaselineMetrics
from coacc.models.entity import SourceAttribution
from coacc.services import lakehouse_query
from coacc.services.lakehouse_entity_service import get_lake_entity
from coacc.services.public_guard import should_hide_person_entities

_DIMENSION_KEY_SQL = {
    "sector": "nullif(trim(sector), '')",
    "region": "coalesce(nullif(trim(department), ''), nullif(trim(city), ''))",
}


def _clean_identifier(raw: str) -> str:
    return re.sub(r"[^0-9]", "", raw or "")


def _contract_awards_glob() -> str | None:
    root = lakehouse_query.lake_root() / "curated" / "table=fct_procurement_contract_awards"
    if not root.exists() or not any(root.glob("*.parquet")):
        return None
    return str(root / "*.parquet")


def _add_candidate(candidates: list[str], value: object) -> None:
    if value is None:
        return
    text = str(value).strip()
    if text and text not in candidates:
        candidates.append(text)


def _entity_candidates(entity_id: str) -> list[str]:
    candidates: list[str] = []
    _add_candidate(candidates, entity_id)

    clean = _clean_identifier(entity_id)
    if clean:
        _add_candidate(candidates, clean)
        _add_candidate(candidates, clean[:9])
        _add_candidate(candidates, f"doc:{clean}")
        _add_candidate(candidates, f"company:{clean}")
        _add_candidate(candidates, f"doc:{clean[:9]}")
        _add_candidate(candidates, f"company:{clean[:9]}")

    entity = get_lake_entity(entity_id, include_person=not should_hide_person_entities())
    if entity is not None:
        _add_candidate(candidates, entity.id)
        for value in (entity.properties.get("document_id"), entity.properties.get("nit")):
            document = _clean_identifier(str(value or ""))
            if not document:
                continue
            _add_candidate(candidates, document)
            _add_candidate(candidates, document[:9])
            _add_candidate(candidates, f"doc:{document}")
            _add_candidate(candidates, f"company:{document}")
            _add_candidate(candidates, f"doc:{document[:9]}")
            _add_candidate(candidates, f"company:{document[:9]}")

    return candidates


def _identity_from_candidates(candidates: list[str]) -> tuple[str | None, str | None, str | None]:
    entity = None
    for candidate in candidates:
        entity = get_lake_entity(candidate, include_person=not should_hide_person_entities())
        if entity is not None:
            break
    else:
        return None, None, None

    name = entity.properties.get("name") or entity.properties.get("razon_social")
    document_id = entity.properties.get("document_id") or entity.properties.get("nit")
    return (
        entity.id,
        str(name) if name is not None else None,
        str(document_id) if document_id is not None else None,
    )


def _row_to_metrics(
    row: dict[str, Any],
    *,
    dimension: str,
    identity: tuple[str | None, str | None, str | None],
) -> BaselineMetrics:
    company_id, company_name, company_document_id = identity
    contract_count = int(row.get("contract_count") or 0)
    total_value = float(row.get("total_value") or 0.0)
    peer_avg_contracts = float(row.get("peer_avg_contracts") or 0.0)
    peer_avg_value = float(row.get("peer_avg_value") or 0.0)
    return BaselineMetrics(
        company_name=company_name or str(row.get("company_name") or ""),
        company_document_id=company_document_id or str(row.get("company_document_id") or ""),
        company_id=company_id or str(row.get("company_id") or ""),
        contract_count=contract_count,
        total_value=total_value,
        peer_count=int(row.get("peer_count") or 0),
        peer_avg_contracts=peer_avg_contracts,
        peer_avg_value=peer_avg_value,
        contract_ratio=contract_count / peer_avg_contracts if peer_avg_contracts > 0 else 0.0,
        value_ratio=total_value / peer_avg_value if peer_avg_value > 0 else 0.0,
        comparison_dimension=dimension,
        comparison_key=str(row.get("comparison_key") or ""),
        sources=[SourceAttribution(database="lake_curated_procurement")],
    )


def lake_baseline(
    entity_id: str,
    *,
    dimension: str,
    limit: int = 50,
) -> list[BaselineMetrics]:
    key_sql = _DIMENSION_KEY_SQL.get(dimension)
    awards_glob = _contract_awards_glob()
    if key_sql is None or awards_glob is None:
        return []

    candidates = _entity_candidates(entity_id)
    if not candidates:
        return []

    supplier_key_sql = """
        coalesce(
            CASE
                WHEN supplier_nit_canonical IS NOT NULL
                    THEN 'company:' || supplier_nit_canonical
                ELSE NULL
            END,
            supplier_entity_id,
            CASE
                WHEN supplier_document_key IS NOT NULL THEN 'doc:' || supplier_document_key
                ELSE NULL
            END,
            supplier_document_digits
        )
    """
    con = lakehouse_query.connect(read_only=True)
    try:
        cursor = con.execute(
            f"""
            WITH candidates(value) AS (
                SELECT unnest(?)
            ),
            awards AS (
                SELECT *
                FROM read_parquet(?)
            ),
            entity_awards AS (
                SELECT
                    {key_sql} AS comparison_key,
                    max(supplier_name) FILTER (WHERE supplier_name IS NOT NULL)
                        AS company_name,
                    max(coalesce(
                        supplier_nit_canonical,
                        supplier_document_key,
                        supplier_document_digits
                    )) AS company_document_id,
                    max({supplier_key_sql}) AS company_id,
                    count(DISTINCT coalesce(contract_id, cast(award_row_id AS VARCHAR)))
                        AS contract_count,
                    coalesce(sum(coalesce(contract_value, 0.0)), 0.0) AS total_value
                FROM awards
                WHERE supplier_nit_canonical IN (SELECT value FROM candidates)
                    OR supplier_document_key IN (SELECT value FROM candidates)
                    OR supplier_document_digits IN (SELECT value FROM candidates)
                    OR supplier_entity_id IN (SELECT value FROM candidates)
                    OR {supplier_key_sql} IN (SELECT value FROM candidates)
                GROUP BY comparison_key
            ),
            peer_supplier_totals AS (
                SELECT
                    {key_sql} AS comparison_key,
                    {supplier_key_sql} AS supplier_key,
                    count(DISTINCT coalesce(contract_id, cast(award_row_id AS VARCHAR)))
                        AS contract_count,
                    coalesce(sum(coalesce(contract_value, 0.0)), 0.0) AS total_value
                FROM awards
                WHERE {key_sql} IS NOT NULL
                    AND {supplier_key_sql} IS NOT NULL
                GROUP BY comparison_key, supplier_key
            ),
            peer_baselines AS (
                SELECT
                    comparison_key,
                    count(*) AS peer_count,
                    avg(contract_count) AS peer_avg_contracts,
                    avg(total_value) AS peer_avg_value
                FROM peer_supplier_totals
                GROUP BY comparison_key
            )
            SELECT
                e.company_name,
                e.company_document_id,
                e.company_id,
                e.comparison_key,
                e.contract_count,
                e.total_value,
                p.peer_count,
                p.peer_avg_contracts,
                p.peer_avg_value
            FROM entity_awards e
            JOIN peer_baselines p USING (comparison_key)
            WHERE e.comparison_key IS NOT NULL
            ORDER BY
                CASE
                    WHEN p.peer_avg_value > 0 THEN e.total_value / p.peer_avg_value
                    ELSE 0
                END DESC,
                CASE
                    WHEN p.peer_avg_contracts > 0
                        THEN e.contract_count / p.peer_avg_contracts
                    ELSE 0
                END DESC,
                e.comparison_key
            LIMIT {int(limit)}
            """,
            [candidates, awards_glob],
        )
        columns = [item[0] for item in cursor.description or []]
        rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    finally:
        con.close()

    identity = _identity_from_candidates(candidates)
    return [_row_to_metrics(row, dimension=dimension, identity=identity) for row in rows]


def lake_all_baselines(entity_id: str) -> list[BaselineMetrics]:
    results: list[BaselineMetrics] = []
    for dimension in _DIMENSION_KEY_SQL:
        results.extend(lake_baseline(entity_id, dimension=dimension))
    return results
