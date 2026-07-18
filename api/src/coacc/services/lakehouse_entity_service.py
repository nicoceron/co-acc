from __future__ import annotations

import re
from typing import Any

from coacc.models.entity import EntityResponse, SourceAttribution
from coacc.models.search import SearchResponse, SearchResult
from coacc.services import lakehouse_query
from coacc.services.public_guard import (
    infer_exposure_tier,
    sanitize_public_properties,
    should_hide_person_entities,
)

_SAFE_LIKE = re.compile(r"([%_\\])")


def _clean_identifier(raw: str) -> str:
    return re.sub(r"[.\-/]", "", raw or "")


def _identifier_core(raw: str) -> str:
    value = (raw or "").strip()
    if ":" in value:
        prefix, suffix = value.split(":", 1)
        if prefix in {"doc", "company", "buyer", "person"}:
            value = suffix
    return _clean_identifier(value)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _table_glob(table: str) -> str | None:
    root = lakehouse_query.lake_root() / "curated" / f"table={table}"
    if not root.exists() or not any(root.glob("*.parquet")):
        return None
    return str(root / "*.parquet")


def _like_pattern(query: str) -> str:
    escaped = _SAFE_LIKE.sub(r"\\\1", query.strip().lower())
    return f"%{escaped}%"


def _coerce_prop(value: object) -> str | float | int | bool | None:
    if value is None or isinstance(value, str | float | int | bool):
        return value
    return str(value)


def _entity_from_row(row: dict[str, Any]) -> EntityResponse:
    label = str(row["entity_label"])
    entity_type = "person" if label == "Person" else "company"
    props = {
        key: _coerce_prop(value)
        for key, value in row.items()
        if key
        not in {
            "entity_id",
            "entity_label",
            "source_list",
            "search_score",
        }
    }
    source_list = row.get("source_list")
    sources = (
        [SourceAttribution(database=str(source)) for source in source_list if str(source).strip()]
        if isinstance(source_list, list)
        else []
    )
    return EntityResponse(
        id=str(row.get("document_id") or _identifier_core(str(row["entity_id"]))),
        type=entity_type,
        entity_label=label,
        identity_quality="exact",
        properties=sanitize_public_properties(props),
        sources=sources,
        is_pep=False,
        exposure_tier=infer_exposure_tier([label]),
    )


def _dimension_selects(include_person: bool = True) -> list[str]:
    selects: list[str] = []
    company_glob = _table_glob("dim_company")
    if company_glob:
        selects.append(f"""
            SELECT
                entity_uid AS entity_id,
                'Company' AS entity_label,
                nit_canonical AS document_id,
                nit_canonical AS nit,
                name_canonical AS name,
                name_canonical AS razon_social,
                'supplier' AS role,
                source_row_count,
                NULL::BIGINT AS contract_count,
                NULL::DOUBLE AS total_contract_value,
                sources AS source_list,
                1.0 AS search_score
            FROM read_parquet({_sql_string(company_glob)})
        """)
    buyer_glob = _table_glob("dim_buyer")
    if buyer_glob:
        selects.append(f"""
            SELECT
                entity_uid AS entity_id,
                'Company' AS entity_label,
                nit_canonical AS document_id,
                nit_canonical AS nit,
                name_canonical AS name,
                name_canonical AS razon_social,
                'buyer' AS role,
                NULL::BIGINT AS source_row_count,
                contract_count,
                total_contract_value,
                sources AS source_list,
                0.95 AS search_score
            FROM read_parquet({_sql_string(buyer_glob)})
        """)
    person_glob = _table_glob("dim_person")
    if include_person and person_glob:
        selects.append(f"""
            SELECT
                entity_uid AS entity_id,
                'Person' AS entity_label,
                cedula_canonical AS document_id,
                NULL::VARCHAR AS nit,
                name_canonical AS name,
                NULL::VARCHAR AS razon_social,
                'person' AS role,
                source_row_count,
                NULL::BIGINT AS contract_count,
                NULL::DOUBLE AS total_contract_value,
                sources AS source_list,
                0.9 AS search_score
            FROM read_parquet({_sql_string(person_glob)})
        """)
    return selects


def _dimension_query(include_person: bool = True) -> str | None:
    selects = _dimension_selects(include_person=include_person)
    if not selects:
        return None
    return " UNION ALL ".join(
        f"SELECT * FROM ({select_sql}) AS dim_{index}"
        for index, select_sql in enumerate(selects)
    )


def get_lake_entity(identifier: str, *, include_person: bool = True) -> EntityResponse | None:
    query = _dimension_query(include_person=include_person)
    if query is None:
        return None
    clean = _identifier_core(identifier)
    con = lakehouse_query.connect(read_only=True)
    try:
        cursor = con.execute(
            f"""
            WITH dims AS ({query})
            SELECT *
            FROM dims
            WHERE entity_id = ?
                OR document_id = ?
                OR (nit IS NOT NULL AND left(nit, 9) = ?)
                OR entity_id = 'doc:' || ?
                OR entity_id = 'company:' || ?
                OR entity_id = 'buyer:' || ?
                OR entity_id = 'person:' || ?
            ORDER BY
                CASE role WHEN 'supplier' THEN 1 WHEN 'buyer' THEN 2 ELSE 3 END,
                source_row_count DESC NULLS LAST,
                contract_count DESC NULLS LAST
            LIMIT 1
            """,
            [identifier, clean, clean, clean, clean, clean, clean],
        )
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [item[0] for item in cursor.description or []]
        return _entity_from_row(dict(zip(columns, row, strict=False)))
    finally:
        con.close()


def resolvable_lake_company_identifiers(identifiers: list[str]) -> set[str] | None:
    """Return input identifiers that resolve to a public company dimension row.

    ``None`` means the company dimensions are unavailable, so callers can retain
    their normal fallback behavior instead of treating every identifier as invalid.
    """
    query = _dimension_query(include_person=False)
    if query is None:
        return None
    originals = list(dict.fromkeys(item for item in identifiers if item.strip()))
    if not originals:
        return set()
    cores = list(dict.fromkeys(_identifier_core(item) for item in originals))
    candidate_ids = list(dict.fromkeys([
        *originals,
        *(f"{prefix}:{core}" for core in cores for prefix in ("doc", "company", "buyer")),
    ]))
    con = lakehouse_query.connect(read_only=True)
    try:
        rows = con.execute(
            f"""
            WITH dims AS ({query})
            SELECT entity_id, document_id, nit
            FROM dims
            WHERE entity_label = 'Company'
              AND (
                entity_id IN (SELECT unnest(?))
                OR document_id IN (SELECT unnest(?))
                OR (nit IS NOT NULL AND left(nit, 9) IN (SELECT unnest(?)))
              )
            """,
            [candidate_ids, cores, cores],
        ).fetchall()
    finally:
        con.close()
    entity_ids = {str(row[0]) for row in rows if row[0] is not None}
    document_ids = {str(row[1]) for row in rows if row[1] is not None}
    nit_roots = {str(row[2])[:9] for row in rows if row[2] is not None}
    return {
        original
        for original in originals
        if original in entity_ids
        or _identifier_core(original) in document_ids
        or _identifier_core(original) in nit_roots
        or any(
            f"{prefix}:{_identifier_core(original)}" in entity_ids
            for prefix in ("doc", "company", "buyer")
        )
    }


def search_lake_entities(
    query_text: str,
    *,
    entity_type: str | None,
    page: int,
    size: int,
) -> SearchResponse:
    include_person = not should_hide_person_entities()
    query = _dimension_query(include_person=include_person)
    if query is None:
        return SearchResponse(results=[], total=0, page=page, size=size)
    clean = _clean_identifier(query_text)
    like = _like_pattern(query_text)
    predicates = [
        """(
            lower(coalesce(name, '')) LIKE ? ESCAPE '\\'
            OR document_id = ?
            OR (nit IS NOT NULL AND left(nit, 9) = ?)
            OR entity_id = 'doc:' || ?
            OR entity_id = 'company:' || ?
            OR entity_id = 'buyer:' || ?
            OR entity_id = 'person:' || ?
        )"""
    ]
    params: list[object] = [like, clean, clean, clean, clean, clean, clean]
    type_filter = (entity_type or "").strip().lower()
    if type_filter:
        if type_filter == "company":
            predicates.append("entity_label = 'Company'")
        elif type_filter == "person":
            predicates.append("entity_label = 'Person'")
        else:
            return SearchResponse(results=[], total=0, page=page, size=size)
    where_sql = " AND ".join(predicates)
    offset = (page - 1) * size
    con = lakehouse_query.connect(read_only=True)
    try:
        total_row = con.execute(
            f"WITH dims AS ({query}) SELECT count(*) FROM dims WHERE {where_sql}",
            params,
        ).fetchone()
        cursor = con.execute(
            f"""
            WITH dims AS ({query})
            SELECT *
            FROM dims
            WHERE {where_sql}
            ORDER BY
                CASE WHEN document_id = ? THEN 0 ELSE 1 END,
                search_score DESC,
                name ASC NULLS LAST,
                entity_id
            LIMIT {int(size)}
            OFFSET {int(offset)}
            """,
            [*params, clean],
        )
        columns = [item[0] for item in cursor.description or []]
        rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    finally:
        con.close()

    results = [
        SearchResult(
            id=str(row.get("document_id") or _identifier_core(str(row["entity_id"]))),
            type="person" if row["entity_label"] == "Person" else "company",
            name=str(row.get("name") or row.get("document_id") or row["entity_id"]),
            score=float(row.get("search_score") or 1.0),
            document=str(row["document_id"]) if row.get("document_id") is not None else None,
            properties=sanitize_public_properties({
                "document_id": _coerce_prop(row.get("document_id")),
                "nit": _coerce_prop(row.get("nit")),
                "role": _coerce_prop(row.get("role")),
            }),
            sources=[
                SourceAttribution(database=str(source))
                for source in (row.get("source_list") or [])
                if str(source).strip()
            ],
            exposure_tier=infer_exposure_tier([str(row["entity_label"])]),
        )
        for row in rows
    ]
    return SearchResponse(
        results=results,
        total=int(total_row[0]) if total_row else 0,
        page=page,
        size=size,
    )
