from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from coacc.models.agent import (
    AgentCitation,
    AgentQueryRequest,
    AgentQueryResponse,
    AgentSubgraph,
    AgentSubgraphEdge,
    AgentSubgraphNode,
)
from coacc.services import lakehouse_query
from coacc.services.lakehouse_anomaly_service import anomaly_case_id, anomaly_score_for_contract
from coacc.services.lakehouse_signal_service import get_lake_case, list_lake_cases

if TYPE_CHECKING:
    from coacc.models.case import CaseResponse

_CONTRACT_RE = re.compile(r"\b(?:CO1[.A-Za-z0-9_-]*|C-[A-Za-z0-9_.=-]+)\b")
_SOURCE_DATASET_IDS = {
    "secop_ii_contracts": "jbjy-vk9h",
    "secop_offers": "wi7w-2nvm",
    "paco_sanctions": "paco_sanctions",
}


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _contract_id_from_question(question: str) -> str | None:
    match = _CONTRACT_RE.search(question)
    return match.group(0) if match else None


def _contract_context(contract_id: str | None) -> dict[str, Any]:
    if not contract_id:
        return {}
    root = lakehouse_query.lake_root() / "curated" / "table=fct_procurement_contract_awards"
    if not root.exists() or not any(root.glob("*.parquet")):
        return {}
    con = lakehouse_query.connect(read_only=True)
    try:
        cursor = con.execute(
            f"""
            SELECT
                contract_id,
                contract_reference,
                buyer_name,
                supplier_name,
                procurement_modality,
                contract_value,
                process_url
            FROM read_parquet({_sql_string(str(root / "*.parquet"))})
            WHERE contract_id = ?
            LIMIT 1
            """,
            [contract_id],
        )
        columns = [item[0] for item in cursor.description or []]
        row = cursor.fetchone()
    finally:
        con.close()
    if row is None:
        return {}
    return dict(zip(columns, row, strict=False))


def _resolve_case(request: AgentQueryRequest) -> CaseResponse | None:
    if request.case_id:
        direct = get_lake_case(request.case_id)
        if direct is not None:
            return direct
        score = anomaly_score_for_contract(request.case_id)
        if score is not None:
            return get_lake_case(anomaly_case_id(score.contract_id))

    contract_id = _contract_id_from_question(request.question)
    if contract_id:
        score = anomaly_score_for_contract(contract_id)
        if score is not None:
            return get_lake_case(anomaly_case_id(score.contract_id))

    cases = list_lake_cases(page=1, size=1).cases
    return get_lake_case(cases[0].id) if cases else None


def _citations(case: CaseResponse) -> list[AgentCitation]:
    citations: list[AgentCitation] = []
    seen: set[tuple[str | None, str | None, str | None]] = set()
    for bundle in case.evidence_bundles:
        for item in bundle.evidence_items:
            key = (item.source_id, item.record_id, item.url)
            if key in seen:
                continue
            seen.add(key)
            citations.append(
                AgentCitation(
                    dataset_id=_SOURCE_DATASET_IDS.get(item.source_id or "", item.source_id),
                    source_id=item.source_id,
                    row_key=item.record_id or item.node_ref or item.label,
                    url=item.url,
                    label=item.label,
                    case_id=case.id,
                    evidence_item_id=item.item_id,
                )
            )
    return citations


def _node(
    nodes: dict[str, AgentSubgraphNode],
    *,
    node_id: str | None,
    label: str | None,
    node_type: str,
) -> None:
    if not node_id or not label:
        return
    nodes.setdefault(node_id, AgentSubgraphNode(id=node_id, label=label, type=node_type))


def _subgraph(case: CaseResponse, context: dict[str, Any]) -> AgentSubgraph:
    nodes: dict[str, AgentSubgraphNode] = {}
    edges: list[AgentSubgraphEdge] = []
    score = case.anomaly_score
    contract_id = score.contract_id if score else str(context.get("contract_id") or "")
    contract_node = f"contract:{contract_id}" if contract_id else None
    _node(nodes, node_id=contract_node, label=contract_id, node_type="contract")
    _node(
        nodes,
        node_id=f"buyer:{context['buyer_name']}" if context.get("buyer_name") else None,
        label=str(context.get("buyer_name") or ""),
        node_type="buyer",
    )
    _node(
        nodes,
        node_id=f"supplier:{context['supplier_name']}" if context.get("supplier_name") else None,
        label=str(context.get("supplier_name") or ""),
        node_type="supplier",
    )
    if score is not None:
        _node(nodes, node_id=score.entity_uid, label=score.entity_uid, node_type="scored_entity")

    if contract_node and context.get("buyer_name"):
        edges.append(
            AgentSubgraphEdge(
                source=f"buyer:{context['buyer_name']}",
                target=contract_node,
                label="buyer",
            )
        )
    if contract_node and context.get("supplier_name"):
        edges.append(
            AgentSubgraphEdge(
                source=contract_node,
                target=f"supplier:{context['supplier_name']}",
                label="supplier",
            )
        )
    for signal in case.signals:
        signal_node = f"signal:{signal.hit_id}"
        _node(nodes, node_id=signal_node, label=signal.title, node_type="signal")
        if contract_node:
            edges.append(
                AgentSubgraphEdge(source=signal_node, target=contract_node, label="flags")
            )
    return AgentSubgraph(
        case_id=case.id,
        title=case.title,
        nodes=list(nodes.values()),
        edges=edges,
    )


def _format_money(value: object) -> str | None:
    if not isinstance(value, str | int | float):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return f"COP {number:,.0f}"


def _answer(case: CaseResponse, context: dict[str, Any], citation_count: int) -> str:
    score = case.anomaly_score
    parts = [f"Encontre el caso {case.id}: {case.title}."]
    if context:
        supplier = context.get("supplier_name") or "proveedor no normalizado"
        buyer = context.get("buyer_name") or "entidad compradora no normalizada"
        contract_id = context.get("contract_id") or (score.contract_id if score else "contrato")
        value = _format_money(context.get("contract_value"))
        value_text = f" por {value}" if value else ""
        parts.append(
            f"El contrato {contract_id} vincula a {buyer} con {supplier}{value_text}."
        )
    if score is not None:
        features = ", ".join(score.top_features) if score.top_features else "sin impulsores"
        parts.append(
            f"El modelo lo prioriza con puntaje {score.score:.3f} "
            f"y confianza {score.score_confidence}; impulsores: {features}."
        )
        if score.prior_sanction_supplier:
            parts.append(
                "El expediente incluye una coincidencia documental con proveedor sancionado."
            )
    if case.narrative_markdown:
        parts.append("La narrativa verificada ya esta precomputada para este caso.")
    if citation_count:
        parts.append(
            f"Devuelvo {citation_count} cita(s) para revisar las filas o enlaces originales."
        )
    parts.append(
        "Esto es priorizacion analitica con datos abiertos, no una afirmacion de delito."
    )
    return " ".join(parts)


def answer_agent_query(request: AgentQueryRequest) -> AgentQueryResponse:
    case = _resolve_case(request)
    if case is None:
        return AgentQueryResponse(
            answer=(
                "No encontre un caso lake-backed para responder con citas. "
                "Genera anomaly_scores o materializa signal_hits antes de consultar el agente."
            ),
            citations=[],
            subgraphs=[],
            related_case_ids=[],
        )
    contract_id = case.anomaly_score.contract_id if case.anomaly_score else None
    context = _contract_context(contract_id)
    citations = _citations(case)
    return AgentQueryResponse(
        answer=_answer(case, context, len(citations)),
        citations=citations,
        subgraphs=[_subgraph(case, context)],
        related_case_ids=[case.id],
    )
