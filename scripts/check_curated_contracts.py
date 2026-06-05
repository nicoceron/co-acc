#!/usr/bin/env python3
"""Validate shipped curated parquet tables against their runtime contracts."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class CuratedContract:
    types: dict[str, str]
    non_null: tuple[str, ...]


CONTRACTS: dict[str, CuratedContract] = {
    "dim_subject_document": CuratedContract(
        types={
            "entity_id": "VARCHAR",
            "document_key": "VARCHAR",
            "display_name": "VARCHAR",
            "source_ids": "VARCHAR",
            "source_row_count": "BIGINT",
        },
        non_null=("entity_id", "document_key", "source_row_count"),
    ),
    "dim_company": CuratedContract(
        types={
            "entity_uid": "VARCHAR",
            "nit_canonical": "VARCHAR",
            "nit_variants": "VARCHAR[]",
            "name_canonical": "VARCHAR",
            "source_row_count": "BIGINT",
        },
        non_null=("entity_uid", "nit_canonical", "name_canonical", "source_row_count"),
    ),
    "dim_buyer": CuratedContract(
        types={
            "entity_uid": "VARCHAR",
            "nit_canonical": "VARCHAR",
            "name_canonical": "VARCHAR",
            "contract_count": "BIGINT",
            "total_contract_value": "DOUBLE",
        },
        non_null=("entity_uid", "nit_canonical", "name_canonical", "contract_count"),
    ),
    "dim_person": CuratedContract(
        types={
            "entity_uid": "VARCHAR",
            "cedula_canonical": "VARCHAR",
            "document_variants": "VARCHAR[]",
            "name_canonical": "VARCHAR",
            "source_row_count": "BIGINT",
        },
        non_null=("entity_uid", "cedula_canonical", "name_canonical", "source_row_count"),
    ),
    "fct_procurement_contract_awards": CuratedContract(
        types={
            "award_row_id": "BIGINT",
            "contract_id": "VARCHAR",
            "supplier_document_key": "VARCHAR",
            "buyer_document_id": "VARCHAR",
            "contract_value": "DOUBLE",
            "signing_date": "DATE",
            "source_id": "VARCHAR",
        },
        non_null=("award_row_id", "contract_id", "source_id"),
    ),
    "signal_feature_procurement_sanctioned_supplier_awarded": CuratedContract(
        types={
            "signal_id": "VARCHAR",
            "entity_id": "VARCHAR",
            "entity_key": "VARCHAR",
            "scope_key": "VARCHAR",
            "risk_signal": "DECIMAL(3,2)",
            "contract_id": "VARCHAR",
            "paco_record_id": "VARCHAR",
            "join_rule": "VARCHAR",
            "evidence_refs": "VARCHAR[]",
        },
        non_null=("signal_id", "entity_id", "entity_key", "scope_key", "contract_id"),
    ),
    "signal_feature_procurement_supplier_concentration_across_entities": CuratedContract(
        types={
            "signal_id": "VARCHAR",
            "entity_id": "VARCHAR",
            "entity_key": "VARCHAR",
            "scope_key": "VARCHAR",
            "risk_signal": "DOUBLE",
            "contract_count": "BIGINT",
            "distinct_buyer_count": "BIGINT",
            "total_contract_value": "DOUBLE",
            "evidence_refs": "VARCHAR[]",
        },
        non_null=("signal_id", "entity_id", "entity_key", "scope_key", "contract_count"),
    ),
    "signal_feature_procurement_contract_value_outlier_by_category": CuratedContract(
        types={
            "signal_id": "VARCHAR",
            "entity_id": "VARCHAR",
            "entity_key": "VARCHAR",
            "scope_key": "VARCHAR",
            "risk_signal": "DOUBLE",
            "contract_id": "VARCHAR",
            "contract_value": "DOUBLE",
            "category_contract_count": "BIGINT",
            "category_z_score": "DOUBLE",
            "median_multiple": "DOUBLE",
            "evidence_refs": "VARCHAR[]",
        },
        non_null=(
            "signal_id",
            "entity_id",
            "entity_key",
            "scope_key",
            "contract_id",
            "contract_value",
            "category_contract_count",
        ),
    ),
    "signal_feature_procurement_repeat_awards_same_supplier": CuratedContract(
        types={
            "signal_id": "VARCHAR",
            "entity_id": "VARCHAR",
            "entity_key": "VARCHAR",
            "scope_key": "VARCHAR",
            "risk_signal": "DOUBLE",
            "buyer_document_id": "VARCHAR",
            "contract_count": "BIGINT",
            "total_contract_value": "DOUBLE",
            "evidence_refs": "VARCHAR[]",
        },
        non_null=("signal_id", "entity_id", "entity_key", "scope_key", "contract_count"),
    ),
    "signal_feature_procurement_buyer_supplier_network_density": CuratedContract(
        types={
            "signal_id": "VARCHAR",
            "entity_id": "VARCHAR",
            "entity_key": "VARCHAR",
            "scope_key": "VARCHAR",
            "risk_signal": "DOUBLE",
            "distinct_buyer_count": "BIGINT",
            "contract_count": "BIGINT",
            "total_contract_value": "DOUBLE",
            "repeated_buyer_count": "BIGINT",
            "repeated_contract_share": "DOUBLE",
            "evidence_refs": "VARCHAR[]",
        },
        non_null=(
            "signal_id",
            "entity_id",
            "entity_key",
            "scope_key",
            "distinct_buyer_count",
            "contract_count",
            "repeated_buyer_count",
        ),
    ),
    "signal_feature_procurement_cartel_risk_cobidding": CuratedContract(
        types={
            "signal_id": "VARCHAR",
            "entity_id": "VARCHAR",
            "entity_key": "VARCHAR",
            "scope_key": "VARCHAR",
            "scope_type": "VARCHAR",
            "risk_signal": "DOUBLE",
            "counterpart_entity_key": "VARCHAR",
            "shared_process_count": "BIGINT",
            "shared_buyer_count": "BIGINT",
            "max_side_share": "DOUBLE",
            "min_side_share": "DOUBLE",
            "evidence_refs": "VARCHAR[]",
        },
        non_null=(
            "signal_id",
            "entity_id",
            "entity_key",
            "scope_key",
            "scope_type",
            "counterpart_entity_key",
            "shared_process_count",
            "shared_buyer_count",
        ),
    ),
    "signal_feature_procurement_payment_plan_anomalies": CuratedContract(
        types={
            "signal_id": "VARCHAR",
            "entity_id": "VARCHAR",
            "entity_key": "VARCHAR",
            "scope_key": "VARCHAR",
            "scope_type": "VARCHAR",
            "risk_signal": "DOUBLE",
            "anomaly_type": "VARCHAR",
            "contract_id": "VARCHAR",
            "contract_value": "DOUBLE",
            "advance_payment_value": "DOUBLE",
            "advance_payment_share": "DOUBLE",
            "paid_value": "DOUBLE",
            "paid_value_share": "DOUBLE",
            "invoiced_value": "DOUBLE",
            "invoiced_value_share": "DOUBLE",
            "evidence_refs": "VARCHAR[]",
        },
        non_null=(
            "signal_id",
            "entity_id",
            "entity_key",
            "scope_key",
            "scope_type",
            "anomaly_type",
            "contract_id",
            "contract_value",
        ),
    ),
    "signal_feature_procurement_contract_suspensions": CuratedContract(
        types={
            "signal_id": "VARCHAR",
            "entity_id": "VARCHAR",
            "entity_key": "VARCHAR",
            "scope_key": "VARCHAR",
            "scope_type": "VARCHAR",
            "risk_signal": "DOUBLE",
            "contract_id": "VARCHAR",
            "contract_value": "DOUBLE",
            "suspension_event_count": "BIGINT",
            "resumption_event_count": "BIGINT",
            "distinct_suspension_dates": "BIGINT",
            "suspension_span_days": "BIGINT",
            "evidence_refs": "VARCHAR[]",
        },
        non_null=(
            "signal_id",
            "entity_id",
            "entity_key",
            "scope_key",
            "scope_type",
            "contract_id",
            "contract_value",
            "suspension_event_count",
            "distinct_suspension_dates",
        ),
    ),
    "signal_feature_procurement_related_companies_shared_officer": CuratedContract(
        types={
            "signal_id": "VARCHAR",
            "entity_id": "VARCHAR",
            "entity_key": "VARCHAR",
            "scope_key": "VARCHAR",
            "risk_signal": "DOUBLE",
            "representative_document_key": "VARCHAR",
            "contract_count": "BIGINT",
            "linked_company_count": "BIGINT",
            "cluster_total_contract_value": "DOUBLE",
            "evidence_refs": "VARCHAR[]",
        },
        non_null=(
            "signal_id",
            "entity_id",
            "entity_key",
            "scope_key",
            "representative_document_key",
            "linked_company_count",
        ),
    ),
}


def _default_lake_root() -> Path:
    configured = os.getenv("COACC_LAKE_ROOT", "").strip()
    return Path(configured) if configured else REPO_ROOT / "lake"


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_list(values: list[str]) -> str:
    return "[" + ", ".join(_sql_string(value) for value in values) + "]"


def _sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    if row is None:
        raise RuntimeError("count query returned no rows")
    return int(row[0])


def validate_table(table: str, *, lake_root: Path) -> list[str]:
    contract = CONTRACTS[table]
    table_root = lake_root / "curated" / f"table={table}"
    files = sorted(str(path) for path in table_root.rglob("*.parquet") if path.is_file())
    if not files:
        return [f"{table}: no parquet files under {table_root}"]

    findings: list[str] = []
    view = "curated_contract_probe"
    with duckdb.connect(database=":memory:") as con:
        con.execute(
            f"CREATE OR REPLACE TEMP VIEW {view} AS "
            f"SELECT * FROM read_parquet({_sql_list(files)}, "
            "union_by_name = true, hive_partitioning = true)"
        )
        row_count = _count(con, f"SELECT count(*) FROM {view}")
        if row_count <= 0:
            findings.append(f"{table}: expected non-empty parquet output")

        schema = {
            str(row[0]): str(row[1]).upper()
            for row in con.execute(f"DESCRIBE SELECT * FROM {view}").fetchall()
        }
        for column, expected_type in contract.types.items():
            actual_type = schema.get(column)
            if actual_type is None:
                findings.append(f"{table}: missing column {column}")
            elif actual_type != expected_type.upper():
                findings.append(
                    f"{table}: column {column} type {actual_type} != {expected_type}"
                )

        for column in contract.non_null:
            if column not in schema:
                continue
            column_sql = _sql_identifier(column)
            empty_guard = (
                f" OR trim({column_sql}) = ''" if schema[column] == "VARCHAR" else ""
            )
            bad_count = _count(
                con,
                f"SELECT count(*) FROM {view} WHERE {column_sql} IS NULL{empty_guard}",
            )
            if bad_count:
                findings.append(f"{table}: {column} has {bad_count} null/empty rows")

    return findings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lake-root",
        type=Path,
        default=_default_lake_root(),
        help="Lake root to inspect. Defaults to COACC_LAKE_ROOT or ./lake.",
    )
    parser.add_argument(
        "--table",
        action="append",
        choices=sorted(CONTRACTS),
        help="Curated table to validate. May be passed more than once.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tables = args.table or sorted(CONTRACTS)
    findings: list[str] = []
    for table in tables:
        findings.extend(validate_table(table, lake_root=args.lake_root))

    if findings:
        for finding in findings:
            print(f"FAIL {finding}")
        return 1
    print(f"PASS {len(tables)} curated contract(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
