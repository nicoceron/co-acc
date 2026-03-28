from __future__ import annotations

import json
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from neo4j import GraphDatabase

ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = ROOT / "api" / "src" / "coacc" / "data" / "watchlists"
OUTPUT_PATH = OUTPUT_DIR / "companies.json"

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "test-password")

LIMIT = 1000
EDGE_VALUE_THRESHOLD = 50_000_000.0
EDGE_COUNT_THRESHOLD = 5
CHUNK_SIZE = 500


@dataclass
class CompanyRow:
    entity_id: str
    name: str
    document_id: str | None
    contract_count: int = 0
    contract_value: float = 0.0
    buyer_ids: set[str] = field(default_factory=set)
    sanction_count: int = 0
    official_officer_count: int = 0
    official_role_count: int = 0
    sensitive_officer_count: int = 0
    sensitive_role_count: int = 0
    official_names: list[str] = field(default_factory=list)
    low_competition_bid_count: int = 0
    low_competition_bid_value: float = 0.0
    direct_invitation_bid_count: int = 0
    funding_overlap_event_count: int = 0
    funding_overlap_total: float = 0.0
    capacity_mismatch_contract_count: int = 0
    capacity_mismatch_contract_value: float = 0.0
    capacity_mismatch_revenue_ratio: float = 0.0
    capacity_mismatch_asset_ratio: float = 0.0
    execution_gap_contract_count: int = 0
    execution_gap_invoice_total: float = 0.0
    commitment_gap_contract_count: int = 0
    commitment_gap_total: float = 0.0
    fiscal_finding_count: int = 0
    fiscal_finding_total: float = 0.0
    interadmin_agreement_count: int = 0
    interadmin_total: float = 0.0
    interadmin_risk_contract_count: int = 0
    suspension_contract_count: int = 0
    suspension_event_count: int = 0
    sanctioned_still_receiving_contract_count: int = 0
    sanctioned_still_receiving_total: float = 0.0
    split_contract_group_count: int = 0
    split_contract_total: float = 0.0

    def to_payload(self) -> dict[str, Any]:
        signal_types = sum(
            1
            for value in [
                self.fiscal_finding_count > 0,
                self.sanction_count > 0,
                self.official_officer_count > 0,
                self.sensitive_officer_count > 0,
                self.low_competition_bid_count > 0 or self.direct_invitation_bid_count > 0,
                self.funding_overlap_event_count > 0,
                self.capacity_mismatch_contract_count > 0,
                self.execution_gap_contract_count > 0,
                self.commitment_gap_contract_count > 0,
                self.suspension_event_count > 0,
                self.sanctioned_still_receiving_contract_count > 0,
                self.split_contract_group_count > 0,
                self.interadmin_agreement_count > 0
                and (
                    self.interadmin_risk_contract_count > 0
                    or self.official_officer_count > 0
                    or self.sanction_count > 0
                ),
            ]
            if value
        )

        suspicion_score = (
            (4 if self.fiscal_finding_count >= 5 else 3 if self.fiscal_finding_count >= 2 else 2 if self.fiscal_finding_count > 0 else 0)
            + (6 if self.sanction_count >= 10 else 5 if self.sanction_count >= 3 else 4 if self.sanction_count > 0 else 0)
            + (4 if self.official_officer_count > 0 else 0)
            + (2 if self.sensitive_officer_count > 0 else 0)
            + (3 if self.low_competition_bid_count >= 5 else 2 if self.low_competition_bid_count >= 2 else 0)
            + (3 if self.funding_overlap_event_count >= 20 else 2 if self.funding_overlap_event_count >= 5 else 1 if self.funding_overlap_event_count > 0 else 0)
            + (
                4
                if self.capacity_mismatch_revenue_ratio >= 10 or self.capacity_mismatch_asset_ratio >= 5
                else 3
                if self.capacity_mismatch_revenue_ratio >= 5 or self.capacity_mismatch_asset_ratio >= 2
                else 2
                if self.capacity_mismatch_revenue_ratio >= 2 or self.capacity_mismatch_asset_ratio >= 1
                else 0
            )
            + (3 if self.execution_gap_contract_count >= 5 else 2 if self.execution_gap_contract_count >= 2 else 0)
            + (3 if self.commitment_gap_contract_count >= 5 else 2 if self.commitment_gap_contract_count >= 2 else 0)
            + (3 if self.suspension_event_count >= 5 else 2 if self.suspension_event_count >= 2 else 1 if self.suspension_event_count > 0 else 0)
            + (5 if self.sanctioned_still_receiving_contract_count > 0 else 0)
            + (6 if self.split_contract_group_count > 0 and self.split_contract_total >= 120_000_000.0 else 5 if self.split_contract_group_count > 0 else 0)
            + (
                4
                if self.interadmin_agreement_count >= 10 and self.interadmin_risk_contract_count >= 3
                else 3
                if self.interadmin_agreement_count >= 3 and self.interadmin_risk_contract_count >= 1
                else 2
                if self.interadmin_agreement_count > 0
                and (
                    self.interadmin_risk_contract_count > 0
                    or self.official_officer_count > 0
                    or self.sanction_count > 0
                )
                else 0
            )
            + (2 if self.contract_count >= 10 else 1 if self.contract_count >= 3 else 0)
        )

        return {
            "entity_id": self.entity_id,
            "name": self.name,
            "document_id": self.document_id,
            "suspicion_score": int(suspicion_score),
            "signal_types": int(signal_types),
            "contract_count": int(self.contract_count),
            "contract_value": float(self.contract_value),
            "buyer_count": len(self.buyer_ids),
            "fiscal_finding_count": int(self.fiscal_finding_count),
            "fiscal_finding_total": float(self.fiscal_finding_total),
            "sanction_count": int(self.sanction_count),
            "official_officer_count": int(self.official_officer_count),
            "official_role_count": int(self.official_role_count),
            "sensitive_officer_count": int(self.sensitive_officer_count),
            "sensitive_role_count": int(self.sensitive_role_count),
            "official_names": self.official_names[:3],
            "low_competition_bid_count": int(self.low_competition_bid_count),
            "low_competition_bid_value": float(self.low_competition_bid_value),
            "direct_invitation_bid_count": int(self.direct_invitation_bid_count),
            "funding_overlap_event_count": int(self.funding_overlap_event_count),
            "funding_overlap_total": float(self.funding_overlap_total),
            "capacity_mismatch_contract_count": int(self.capacity_mismatch_contract_count),
            "capacity_mismatch_contract_value": float(self.capacity_mismatch_contract_value),
            "capacity_mismatch_revenue_ratio": float(self.capacity_mismatch_revenue_ratio),
            "capacity_mismatch_asset_ratio": float(self.capacity_mismatch_asset_ratio),
            "execution_gap_contract_count": int(self.execution_gap_contract_count),
            "execution_gap_invoice_total": float(self.execution_gap_invoice_total),
            "commitment_gap_contract_count": int(self.commitment_gap_contract_count),
            "commitment_gap_total": float(self.commitment_gap_total),
            "interadmin_agreement_count": int(self.interadmin_agreement_count),
            "interadmin_total": float(self.interadmin_total),
            "interadmin_risk_contract_count": int(self.interadmin_risk_contract_count),
            "suspension_contract_count": int(self.suspension_contract_count),
            "suspension_event_count": int(self.suspension_event_count),
            "sanctioned_still_receiving_contract_count": int(self.sanctioned_still_receiving_contract_count),
            "sanctioned_still_receiving_total": float(self.sanctioned_still_receiving_total),
            "split_contract_group_count": int(self.split_contract_group_count),
            "split_contract_total": float(self.split_contract_total),
        }


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def fetch_rows(driver: Any, query: str, **params: Any) -> list[dict[str, Any]]:
    with driver.session() as session:
        result = session.run(query, params)
        return [record.data() for record in result]


def fetch_interesting_company_ids(driver: Any) -> set[str]:
    queries = [
        "MATCH (c:Company)-[:SANCIONADA]->(:Sanction) RETURN DISTINCT elementId(c) AS entity_id",
        (
            "MATCH (p:Person)-[:OFFICER_OF]->(c:Company) "
            "WHERE EXISTS { MATCH (p)-[:RECIBIO_SALARIO]->(:PublicOffice) } "
            "RETURN DISTINCT elementId(c) AS entity_id"
        ),
        "MATCH (c:Company)-[:TIENE_HALLAZGO]->(:Finding) RETURN DISTINCT elementId(c) AS entity_id",
        "MATCH (:Company)-[:CELEBRO_CONVENIO_INTERADMIN]->(c:Company) RETURN DISTINCT elementId(c) AS entity_id",
        "MATCH (c:Company)-[:DECLARO_FINANZAS]->(:Finance {type:'SUPERSOC_TOP_COMPANY'}) RETURN DISTINCT elementId(c) AS entity_id",
    ]
    entity_ids: set[str] = set()
    for query in queries:
        entity_ids.update(
            str(row["entity_id"])
            for row in fetch_rows(driver, query)
            if row.get("entity_id")
        )
    return entity_ids


def build_company_snapshot() -> list[dict[str, Any]]:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        interesting_company_ids = fetch_interesting_company_ids(driver)
        companies: dict[str, CompanyRow] = {}

        with driver.session(fetch_size=2_000) as session:
            result = session.run(
                """
                MATCH ()-[award:CONTRATOU]->(c:Company)
                RETURN elementId(c) AS entity_id,
                       coalesce(c.razon_social, c.name, c.document_id, c.nit) AS name,
                       coalesce(c.document_id, c.nit) AS document_id,
                       coalesce(award.contract_count, 1) AS contract_count,
                       coalesce(award.total_value, 0.0) AS total_value,
                       trim(coalesce(award.buyer_document_id, award.buyer_name, '')) AS buyer_id,
                       coalesce(award.offer_count, 0) AS offer_count,
                       coalesce(award.direct_invitation, false) AS direct_invitation,
                       coalesce(award.invoice_total_value, 0.0) AS invoice_total_value,
                       coalesce(award.execution_actual_progress_max, 0.0) AS execution_actual_progress_max,
                       coalesce(award.commitment_total_value, 0.0) AS commitment_total_value,
                       coalesce(award.suspension_event_count, 0) AS suspension_event_count,
                       coalesce(award.average_value, 0.0) AS average_value
                """,
            )

            for record in result:
                row = record.data()
                entity_id = str(row["entity_id"])
                contract_count = int(row.get("contract_count") or 0)
                total_value = float(row.get("total_value") or 0.0)
                offer_count = int(row.get("offer_count") or 0)
                direct_invitation = bool(row.get("direct_invitation"))
                execution_gap = (
                    float(row.get("invoice_total_value") or 0.0) > 0.0
                    and float(row.get("execution_actual_progress_max") or 0.0) < 25.0
                )
                commitment_gap = (
                    float(row.get("commitment_total_value") or 0.0) > 0.0
                    and float(row.get("invoice_total_value") or 0.0)
                    > float(row.get("commitment_total_value") or 0.0) * 1.2
                )
                split_contract = (
                    0.0 <= float(row.get("average_value") or 0.0) <= 80_000.0
                    and contract_count >= 3
                    and total_value >= 200_000.0
                )
                low_competition = offer_count > 0 and (offer_count <= 2 or direct_invitation)
                risky_edge = (
                    total_value >= EDGE_VALUE_THRESHOLD
                    or contract_count >= EDGE_COUNT_THRESHOLD
                    or execution_gap
                    or commitment_gap
                    or int(row.get("suspension_event_count") or 0) > 0
                    or split_contract
                    or low_competition
                    or entity_id in interesting_company_ids
                )
                if not risky_edge:
                    continue

                company = companies.setdefault(
                    entity_id,
                    CompanyRow(
                        entity_id=entity_id,
                        name=str(row.get("name") or entity_id),
                        document_id=str(row["document_id"]) if row.get("document_id") else None,
                    ),
                )
                company.contract_count += contract_count
                company.contract_value += total_value
                if row.get("buyer_id"):
                    company.buyer_ids.add(str(row["buyer_id"]))
                if low_competition:
                    company.low_competition_bid_count += contract_count
                    company.low_competition_bid_value += total_value
                if direct_invitation:
                    company.direct_invitation_bid_count += contract_count
                if execution_gap:
                    company.execution_gap_contract_count += contract_count
                    company.execution_gap_invoice_total += float(row.get("invoice_total_value") or 0.0)
                if commitment_gap:
                    company.commitment_gap_contract_count += contract_count
                    company.commitment_gap_total += float(row.get("invoice_total_value") or 0.0) - float(
                        row.get("commitment_total_value") or 0.0
                    )
                if int(row.get("suspension_event_count") or 0) > 0:
                    company.suspension_contract_count += contract_count
                    company.suspension_event_count += int(row.get("suspension_event_count") or 0)
                if split_contract:
                    company.split_contract_group_count += 1
                    company.split_contract_total += total_value

        if not companies:
            return []

        entity_ids = list(companies)

        for chunk in chunked(entity_ids, CHUNK_SIZE):
            for row in fetch_rows(
                driver,
                """
                MATCH (c:Company)-[:SANCIONADA]->(s:Sanction)
                WHERE elementId(c) IN $ids
                RETURN elementId(c) AS entity_id,
                       count(DISTINCT s) AS sanction_count
                """,
                ids=chunk,
            ):
                companies[row["entity_id"]].sanction_count = int(row.get("sanction_count") or 0)

            for row in fetch_rows(
                driver,
                """
                MATCH (p:Person)-[:OFFICER_OF]->(c:Company)
                WHERE elementId(c) IN $ids
                  AND EXISTS { MATCH (p)-[:RECIBIO_SALARIO]->(:PublicOffice) }
                OPTIONAL MATCH (p)-[:RECIBIO_SALARIO]->(o:PublicOffice)
                RETURN elementId(c) AS entity_id,
                       count(DISTINCT p) AS official_officer_count,
                       count(DISTINCT o) AS official_role_count,
                       count(DISTINCT CASE WHEN coalesce(o.sensitive_position, false) THEN p END) AS sensitive_officer_count,
                       count(DISTINCT CASE WHEN coalesce(o.sensitive_position, false) THEN o END) AS sensitive_role_count,
                       collect(DISTINCT coalesce(p.name, p.nombre, p.document_id))[0..3] AS official_names
                """,
                ids=chunk,
            ):
                company = companies[row["entity_id"]]
                company.official_officer_count = int(row.get("official_officer_count") or 0)
                company.official_role_count = int(row.get("official_role_count") or 0)
                company.sensitive_officer_count = int(row.get("sensitive_officer_count") or 0)
                company.sensitive_role_count = int(row.get("sensitive_role_count") or 0)
                company.official_names = [str(name) for name in row.get("official_names") or [] if name]

            for row in fetch_rows(
                driver,
                """
                MATCH (c:Company)-[:SUMINISTRO]->(f:Finance {type:'SGR_EXPENSE_EXECUTION'})
                WHERE elementId(c) IN $ids
                RETURN elementId(c) AS entity_id,
                       count(DISTINCT f) AS funding_overlap_event_count,
                       coalesce(sum(coalesce(f.value, 0.0)), 0.0) AS funding_overlap_total
                """,
                ids=chunk,
            ):
                company = companies[row["entity_id"]]
                company.funding_overlap_event_count = int(row.get("funding_overlap_event_count") or 0)
                company.funding_overlap_total = float(row.get("funding_overlap_total") or 0.0)

            for row in fetch_rows(
                driver,
                """
                MATCH (c:Company)-[:DECLARO_FINANZAS]->(f:Finance {type:'SUPERSOC_TOP_COMPANY'})
                WHERE elementId(c) IN $ids
                RETURN elementId(c) AS entity_id,
                       max(coalesce(f.operating_revenue_current, 0.0)) AS operating_revenue_current,
                       max(coalesce(f.total_assets_current, 0.0)) AS total_assets_current
                """,
                ids=chunk,
            ):
                company = companies[row["entity_id"]]
                revenue = float(row.get("operating_revenue_current") or 0.0)
                assets = float(row.get("total_assets_current") or 0.0)
                company.capacity_mismatch_contract_count = company.contract_count
                company.capacity_mismatch_contract_value = company.contract_value
                company.capacity_mismatch_revenue_ratio = company.contract_value / revenue if revenue > 0 else 0.0
                company.capacity_mismatch_asset_ratio = company.contract_value / assets if assets > 0 else 0.0
                if company.capacity_mismatch_revenue_ratio < 2.0 and company.capacity_mismatch_asset_ratio < 1.0:
                    company.capacity_mismatch_contract_count = 0
                    company.capacity_mismatch_contract_value = 0.0
                    company.capacity_mismatch_revenue_ratio = 0.0
                    company.capacity_mismatch_asset_ratio = 0.0

            for row in fetch_rows(
                driver,
                """
                MATCH (c:Company)-[:TIENE_HALLAZGO]->(finding:Finding)
                WHERE elementId(c) IN $ids
                RETURN elementId(c) AS entity_id,
                       count(DISTINCT finding) AS fiscal_finding_count,
                       coalesce(sum(coalesce(finding.amount, 0.0)), 0.0) AS fiscal_finding_total
                """,
                ids=chunk,
            ):
                company = companies[row["entity_id"]]
                company.fiscal_finding_count = int(row.get("fiscal_finding_count") or 0)
                company.fiscal_finding_total = float(row.get("fiscal_finding_total") or 0.0)

            for row in fetch_rows(
                driver,
                """
                MATCH (:Company)-[ia:CELEBRO_CONVENIO_INTERADMIN]->(c:Company)
                WHERE elementId(c) IN $ids
                RETURN elementId(c) AS entity_id,
                       count(DISTINCT ia.summary_id) AS interadmin_agreement_count,
                       coalesce(sum(coalesce(ia.total_value, 0.0)), 0.0) AS interadmin_total
                """,
                ids=chunk,
            ):
                company = companies[row["entity_id"]]
                company.interadmin_agreement_count = int(row.get("interadmin_agreement_count") or 0)
                company.interadmin_total = float(row.get("interadmin_total") or 0.0)

            sanctioned_ids = [entity_id for entity_id in chunk if companies[entity_id].sanction_count > 0]
            if sanctioned_ids:
                for row in fetch_rows(
                    driver,
                    """
                    MATCH (c:Company)-[:SANCIONADA]->(s:Sanction)
                    WHERE elementId(c) IN $ids
                      AND s.date_start IS NOT NULL
                      AND trim(s.date_start) <> ''
                    MATCH ()-[award:CONTRATOU]->(c)
                    WHERE coalesce(award.last_date, award.first_date) IS NOT NULL
                      AND coalesce(award.last_date, award.first_date) >= s.date_start
                      AND (
                        s.date_end IS NULL
                        OR trim(coalesce(s.date_end, '')) = ''
                        OR coalesce(award.last_date, award.first_date) <= s.date_end
                      )
                    WITH DISTINCT c, award
                    RETURN elementId(c) AS entity_id,
                           toInteger(sum(coalesce(award.contract_count, 1))) AS sanctioned_still_receiving_contract_count,
                           coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS sanctioned_still_receiving_total
                    """,
                    ids=sanctioned_ids,
                ):
                    company = companies[row["entity_id"]]
                    company.sanctioned_still_receiving_contract_count = int(
                        row.get("sanctioned_still_receiving_contract_count") or 0
                    )
                    company.sanctioned_still_receiving_total = float(
                        row.get("sanctioned_still_receiving_total") or 0.0
                    )

        for company in companies.values():
            if company.interadmin_agreement_count > 0 and (
                company.execution_gap_contract_count > 0
                or company.commitment_gap_contract_count > 0
                or company.suspension_contract_count > 0
                or company.official_officer_count > 0
                or company.sanction_count > 0
            ):
                company.interadmin_risk_contract_count = (
                    company.execution_gap_contract_count
                    + company.commitment_gap_contract_count
                    + company.suspension_contract_count
                )

        rows = [company.to_payload() for company in companies.values()]
        rows.sort(
            key=lambda row: (
                -int(row["suspicion_score"]),
                -int(row["fiscal_finding_count"]),
                -float(row["capacity_mismatch_revenue_ratio"]),
                -float(row["capacity_mismatch_asset_ratio"]),
                -float(row["contract_value"]),
                -int(row["sanction_count"]),
                -int(row["low_competition_bid_count"]),
                str(row["name"]),
            )
        )
        return rows[:LIMIT]
    finally:
        driver.close()


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    companies = build_company_snapshot()
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "companies": companies,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(json.dumps({"companies": len(companies), "path": str(OUTPUT_PATH)}))


if __name__ == "__main__":
    main()
