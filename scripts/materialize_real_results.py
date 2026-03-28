#!/usr/bin/env python3
"""Build a compact materialized results pack from the live public graph."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

DEFAULT_API_BASE = "http://localhost:8000"
DEFAULT_OUTPUT = "frontend/public/data/materialized-results.json"
DEFAULT_MIRROR = "audit-results/materialized-results/latest/materialized-results.json"

COMPANY_LIMIT = 1000
PEOPLE_LIMIT = 200
BUYER_LIMIT = 200
TERRITORY_LIMIT = 100
FEATURED_COMPANY_LIMIT = 48
FEATURED_PERSON_LIMIT = 36
PRACTICE_GROUP_LIMIT = 16
PRACTICE_GROUP_ITEM_LIMIT = 12
DETAIL_GRAPH_DEPTH = 1
DETAIL_GRAPH_NODE_LIMIT = 140
DETAIL_GRAPH_EDGE_LIMIT = 220
DETAIL_GRAPH_NODE_TYPE_LIMITS = {
    "education": 24,
}
DETAIL_GRAPH_EDGE_TYPE_LIMITS = {
    "MANTIENE_A": 24,
}
INVESTIGATION_LIMIT = 17
GENERATED_INVESTIGATION_LIMIT = 4
UNGRD_STRUCTURED_EVIDENCE_PATH = Path(
    "audit-results/investigations/ungrd-public-evidence-2026-03-27/structured-evidence.json"
)
UNGRD_CASE_IDS = {
    "olmedo_lopez_ungrd_bulletin_record",
    "sneyder_pinilla_ungrd_bulletin_record",
    "carlos_ramon_gonzalez_ungrd_bulletin_record",
    "luis_carlos_barreto_ungrd_bulletin_record",
    "sandra_ortiz_ungrd_bulletin_record",
    "maria_alejandra_benavides_ungrd_bulletin_record",
}

DATASET_URLS = {
    "company_registry_c82u": "https://www.datos.gov.co/d/c82u-588k",
    "fiscal_responsibility": "https://www.datos.gov.co/d/jr8e-e8tu",
    "paco_sanctions": "https://portal.paco.gov.co/index.php?pagina=descargarDatos",
    "secop_document_archives": "https://www.datos.gov.co/d/dmgg-8hin",
    "secop_payment_plans": "https://www.datos.gov.co/d/uymx-8p3j",
    "secop_i_historical_processes": "https://www.datos.gov.co/d/qddk-cgux",
    "secop_i_resource_origins": "https://www.datos.gov.co/d/3xwx-53wt",
    "secop_interadmin_agreements": "https://www.datos.gov.co/d/s484-c9k3",
    "sigep_public_servants": "https://www.datos.gov.co/d/2jzx-383z",
    "sigep_sensitive_positions": "https://www.datos.gov.co/d/5u9e-g5w9",
    "siri_antecedents": "https://www.datos.gov.co/d/iaeu-rcn6",
}

SIGNAL_LABELS = {
    "budget_execution_discrepancy": "Facturación o pagos por delante de la ejecución",
    "candidate_supplier_overlap": "Candidatura y contratación en la misma persona",
    "company_capacity_mismatch": "Contratación muy superior al tamaño financiero reportado",
    "company_donor_vendor_overlap": "Empresa donante y contratista",
    "contract_suspension_stacking": "Suspensiones repetidas en contratos públicos",
    "disclosure_risk_stack": "Declaraciones con referencias corporativas o conflictos",
    "donor_official_vendor_loop": "Ruta donante-funcionario-proveedor",
    "donor_supplier_overlap": "Donante que también aparece como proveedor",
    "education_control_capture": "Control institucional concentrado con alias contractuales",
    "funding_spike_then_awards": "Pico de recursos públicos antes de nuevos contratos",
    "funding_overlap": "Cruce entre financiación política y contratación",
    "interadministrative_channel_stacking": "Convenios interadministrativos apilados con contratación regular",
    "invoice_execution_gap": "Facturas sin avance material suficiente",
    "low_competition_bidding": "Baja competencia o invitación directa",
    "payment_supervision_risk_stack": "Supervisión de pagos sobre contratos riesgosos",
    "public_money_channel_stacking": "Canales públicos múltiples sobre el mismo actor",
    "public_official_supplier_overlap": "Proveedor con directivo o vínculo en cargo público",
    "sanctioned_person_exposure_stack": "Sanciones oficiales con exposición pública",
    "sanctioned_still_receiving": "Proveedor sancionado que siguió recibiendo contratos",
    "sanctioned_health_operator_overlap": "Prestador de salud con sanciones",
    "sanctioned_supplier_record": "Proveedor con antecedentes sancionatorios",
    "sensitive_public_official_supplier_overlap": "Proveedor ligado a cargo sensible",
    "split_contracts_below_threshold": "Paquetes repetidos de contratos bajo umbral",
    "shared_officer_supplier_network": "Red compartida de directivos en proveedores",
}

METRIC_LABELS = {
    "amount_total": "valor total",
    "archive_assignment_contract_count": "contratos con designación o delegación",
    "archive_assignment_document_total": "documentos de designación o delegación",
    "archive_contract_count": "contratos con expediente documental",
    "archive_document_total": "referencias documentales SECOP",
    "archive_payment_document_total": "soportes de pago",
    "archive_payment_contract_count": "contratos con soportes de pago",
    "archive_report_document_total": "informes o actas de supervisión",
    "archive_resume_contract_count": "contratos con hoja de vida",
    "archive_resume_document_total": "hojas de vida",
    "archive_start_record_contract_count": "contratos con acta de inicio",
    "archive_start_record_document_total": "actas u oficios de inicio",
    "archive_supervision_contract_count": "contratos con documentos de supervisión",
    "archive_supervision_document_total": "documentos de supervisión",
    "buyer_count": "compradores",
    "candidate_count": "candidaturas",
    "candidacy_count": "candidaturas",
    "commitment_gap_contract_count": "contratos con brecha presupuestal",
    "contract_count": "contratos",
    "contract_value": "valor contractual",
    "contract_total": "valor contractual",
    "direct_invitation_bid_count": "invitaciones directas",
    "donation_count": "donaciones",
    "education_alias_count": "alias institucionales",
    "education_director_count": "directivos institucionales",
    "education_family_tie_count": "vínculos familiares internos",
    "education_procurement_link_count": "convenios o enlaces contractuales",
    "education_procurement_total": "valor contractual vinculado",
    "execution_gap_contract_count": "contratos con brecha de ejecución",
    "execution_gap_invoice_total": "facturación adelantada",
    "funding_overlap_event_count": "eventos de cruce financiero",
    "historical_contract_count": "contratos históricos",
    "historical_contract_value": "valor histórico",
    "historical_resource_origin_total": "valor con origen de recursos",
    "historical_with_origin_count": "contratos históricos con origen de recursos",
    "interadmin_agreement_count": "convenios interadministrativos",
    "interadmin_total": "valor interadministrativo",
    "invoice_total": "facturación",
    "linked_supplier_company_count": "empresas proveedoras enlazadas",
    "low_competition_bid_count": "procesos de baja competencia",
    "official_case_bulletin_count": "boletines oficiales",
    "official_officer_count": "directivos públicos",
    "official_role_count": "roles públicos",
    "office_count": "cargos públicos",
    "payment_supervision_company_count": "proveedores supervisados",
    "payment_supervision_contract_count": "contratos supervisados",
    "payment_supervision_contract_value": "valor supervisado",
    "payment_supervision_count": "contratos supervisados",
    "payment_supervision_discrepancy_contract_count": "contratos supervisados con brecha",
    "payment_supervision_pending_contract_count": "contratos supervisados con pagos pendientes",
    "payment_supervision_risk_contract_count": "contratos supervisados con riesgo",
    "payment_supervision_suspension_contract_count": "contratos supervisados suspendidos",
    "person_sanction_count": "sanciones personales",
    "risk_signal": "señal de riesgo",
    "sanction_count": "sanciones",
    "sanctioned_still_receiving_contract_count": "contratos en ventana de sanción",
    "sanctioned_still_receiving_total": "valor contratado en ventana de sanción",
    "sensitive_officer_count": "directivos en cargos sensibles",
    "split_contract_group_count": "grupos bajo umbral",
    "stack_signal_types": "señales apiladas",
    "supplier_contract_count": "contratos como proveedor",
    "supplier_contract_value": "valor contratado",
    "suspension_contract_count": "contratos suspendidos",
}

IGNORED_PATTERN_KEYS = {
    "company_identifier",
    "company_name",
    "evidence_count",
    "evidence_refs",
    "window_end",
    "window_start",
}

TEXT_REPLACEMENTS = str.maketrans(
    {
        "\u0085": "...",
        "\u0091": "'",
        "\u0092": "'",
        "\u0093": '"',
        "\u0094": '"',
        "\u0095": "-",
        "\u0096": " - ",
        "\u0097": " - ",
    }
)


def fetch_json(api_base: str, path: str) -> Any:
    url = f"{api_base.rstrip('/')}{path}"
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "coacc-materializer/1.0",
        },
    )
    try:
        with urlopen(request, timeout=60) as response:  # noqa: S310
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"{url} failed with HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"{url} failed: {exc.reason}") from exc


def progress(message: str) -> None:
    print(f"[materialize] {message}", flush=True)


def normalize_ref(value: object) -> str:
    text = str(value or "").strip()
    digits = re.sub(r"[^\d]", "", text)
    return digits or text.upper()


def format_signal_label(signal_id: str) -> str:
    return SIGNAL_LABELS.get(signal_id, signal_id.replace("_", " ").strip().capitalize())


def compact_money(value: float | int) -> str:
    amount = float(value or 0.0)
    if amount >= 1_000_000_000:
        return f"COP {amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"COP {amount / 1_000_000:.1f}M"
    if amount >= 1_000:
        return f"COP {amount / 1_000:.1f}K"
    return f"COP {amount:.0f}"


def dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        clean = value.strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        output.append(clean)
    return output


def load_optional_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def load_ungrd_structured_evidence() -> dict[str, Any] | None:
    return load_optional_json(UNGRD_STRUCTURED_EVIDENCE_PATH)


def ungrd_document_sources(bundle: dict[str, Any] | None) -> list[str]:
    if not bundle:
        return []
    sources: list[str] = []
    for page in bundle.get("links", {}).get("pages", []) or []:
        if isinstance(page, dict):
            url = str(page.get("url") or "").strip()
            if url:
                sources.append(url)
    for document in bundle.get("documents", []) or []:
        if isinstance(document, dict):
            url = str(document.get("source_url") or "").strip()
            if url:
                sources.append(url)
    return dedupe_strings(sources)


def build_ungrd_document_context(
    case: dict[str, Any],
    bundle: dict[str, Any] | None,
) -> tuple[list[str], list[str], list[str], list[str]]:
    if str(case.get("case_id") or "") not in UNGRD_CASE_IDS:
        return [], [], [], []

    metrics = case.get("metrics") or {}
    summary = (bundle or {}).get("summary") or {}
    network_summary = (bundle or {}).get("network_summary") or {}
    documents = [doc for doc in ((bundle or {}).get("documents") or []) if isinstance(doc, dict)]

    findings: list[str] = []
    verified: list[str] = []
    open_questions: list[str] = []

    if summary:
        verified.append(
            "bundle documental público · "
            f"{int(summary.get('monitor_pdf_count') or 0)} PDF(s) de Monitor Ciudadano, "
            f"{int(summary.get('secop_community_link_count') or 0)} enlaces SECOP Community, "
            f"{int(summary.get('contratos_gov_link_count') or 0)} enlaces contratos.gov y "
            f"{int(summary.get('google_drive_link_count') or 0)} carpetas Drive desde el portal de contratos de la UNGRD"
        )
    if network_summary:
        organizations = [str(value) for value in (network_summary.get("organizations") or []) if str(value).strip()]
        reference_codes = [str(value) for value in (network_summary.get("reference_codes") or []) if str(value).strip()]
        nits = [str(value) for value in (network_summary.get("nits") or []) if str(value).strip()]
        named_people = [str(value) for value in (network_summary.get("named_people") or []) if str(value).strip()]
        emails = [str(value) for value in (network_summary.get("emails") or []) if str(value).strip()]
        if organizations:
            verified.append(
                "cadena documental detectada · " + " -> ".join(organizations[:4])
            )
        if reference_codes:
            verified.append(
                "referencias contractuales visibles · " + ", ".join(reference_codes[:4])
            )
        if nits:
            verified.append(
                "NIT visibles en documentos públicos · " + ", ".join(nits[:4])
            )
        if named_people:
            verified.append(
                "firmantes o actores visibles · " + ", ".join(named_people[:4])
            )
        if emails:
            verified.append(
                "correo visible en soporte contractual · " + ", ".join(emails[:2])
            )
        findings.append(
            "El paquete documental ya no es solo boletín: contiene soportes contractuales públicos donde se ve la cadena UNGRD/FNGRD -> Fiduprevisora -> IMPOAMERICANA dentro del caso carrotanques."
        )
    for document in documents[:3]:
        verified.append(
            "documento público · "
            f"{document.get('filename')} · {document.get('doc_type')} · "
            f"{int(document.get('page_count') or 0)} página(s)"
        )

    case_id = str(case.get("case_id") or "")
    if case_id == "olmedo_lopez_ungrd_bulletin_record":
        findings.append(
            "El cruce adicional actual solo llega a un nodo personal homónimo, sin contratación pública enlazada en el grafo."
        )
        verified.append("cruce probable · nodo personal homónimo con documento `98538265` · sin contratos asociados")
        open_questions.append(
            "cierre de identidad · falta un documento oficial con número de identificación para confirmar si el homónimo `98538265` es el mismo actor del expediente"
        )
    elif case_id == "sneyder_pinilla_ungrd_bulletin_record":
        findings.append(
            "El expediente ya toca un clúster de contratista-persona natural por nombre exacto, pero todavía no hay cierre oficial de identidad."
        )
        verified.append(
            "cruce probable · contratista-persona natural `1101200853` / `11012008531` · "
            f"{int(metrics.get('supplier_contract_count') or 0)} contratos por "
            f"{compact_money(metrics.get('supplier_contract_value') or 0)}"
        )
        verified.append(
            "referencias visibles · `CO1.PCCNTR.2391534`, `819-2021`, `CO1.PCCNTR.3454219`, `0756-2022`, "
            "`OPS-232-2012`, `OPS-007-2012`"
        )
        open_questions.append(
            "identidad del contratista · el puente actual es solo de nombre exacto en forma de proveedor-persona natural"
        )
    elif case_id == "luis_carlos_barreto_ungrd_bulletin_record":
        findings.append(
            "El mismo nombre aparece como contratista-persona natural en múltiples alcaldías y entidades, pero ese cruce aún no es prueba de identidad."
        )
        verified.append(
            "cruce probable · contratista-persona natural `11257105` · "
            f"{int(metrics.get('supplier_contract_count') or 0)} contratos por "
            f"{compact_money(metrics.get('supplier_contract_value') or 0)}"
        )
        verified.append(
            "compradores visibles · Fusagasugá, Soacha, Cáqueza, Fuquene y otros entes locales"
        )
        open_questions.append(
            "cierre de identidad · falta documento judicial o administrativo que confirme que el contratista `11257105` y el exdirector de la UNGRD son la misma persona"
        )
    elif case_id == "maria_alejandra_benavides_ungrd_bulletin_record":
        findings.append(
            "Este es el cruce contextual más fuerte del grupo: el nombre exacto aparece como contratista-persona natural del Ministerio de Hacienda, la misma entidad mencionada en el boletín."
        )
        verified.append(
            "cruce probable · contratista-persona natural `1020785233` con `MINISTERIO DE HACIENDA Y CREDITO PUBLICO` · "
            f"{int(metrics.get('supplier_contract_count') or 0)} contratos por "
            f"{compact_money(metrics.get('supplier_contract_value') or 0)}"
        )
        verified.append(
            "referencias visibles · `CO1.PCCNTR.1451509`, `3.151-2020`, `CO1.PCCNTR.2163423`, `3.006-2021`, `CO1.PCCNTR.3304969`"
        )
        open_questions.append(
            "cierre de identidad · sigue faltando un documento oficial del expediente que publique el número de identificación de la exasesora"
        )
    elif case_id == "carlos_ramon_gonzalez_ungrd_bulletin_record":
        verified.append("sin cruce estructurado adicional · por ahora el grafo solo reproduce el boletín oficial")
        open_questions.append(
            "siguiente capa · buscar resoluciones, autos o piezas judiciales que expongan documentos, dependencias o trazas contractuales adicionales"
        )
    elif case_id == "sandra_ortiz_ungrd_bulletin_record":
        verified.append("sin cruce estructurado adicional · por ahora el grafo solo reproduce el boletín oficial")
        open_questions.append(
            "siguiente capa · buscar piezas judiciales o administrativas con más identificadores y conexiones contractuales"
        )

    return findings, verified, open_questions, ungrd_document_sources(bundle)


def normalize_text(value: str) -> str:
    cleaned = value.translate(TEXT_REPLACEMENTS)
    cleaned = re.sub(r"[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f]", " ", cleaned)
    return re.sub(r"\s{2,}", " ", cleaned).strip()


def sanitize_payload(value: Any) -> Any:
    if isinstance(value, str):
        return normalize_text(value)
    if isinstance(value, list):
        return [sanitize_payload(item) for item in value]
    if isinstance(value, dict):
        return {key: sanitize_payload(item) for key, item in value.items()}
    return value


def build_validation_index(cases: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for case in cases:
        key = normalize_ref(case.get("entity_ref"))
        if key:
            index[key].append(case)
    return index


def entity_ref(row: dict[str, Any]) -> str:
    return normalize_ref(
        row.get("document_id")
        or row.get("case_person_id")
        or row.get("entity_id")
        or row.get("name")
    )


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (value or "").lower()).strip("-")
    return slug or "investigation"


def entity_validation_titles(
    row: dict[str, Any],
    validation_index: dict[str, list[dict[str, Any]]],
) -> list[str]:
    return [case.get("title") for case in validation_index.get(entity_ref(row), []) if case.get("title")]


def extract_alert_types(row: dict[str, Any]) -> list[str]:
    return dedupe_strings(
        [
            str(alert.get("alert_type") or "").strip()
            for alert in row.get("alerts") or []
            if str(alert.get("alert_type") or "").strip()
        ]
    )


def select_diverse_rows(
    rows: list[dict[str, Any]],
    limit: int,
    validation_index: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen_entities: set[str] = set()
    covered_practices: set[str] = set()

    def add(row: dict[str, Any]) -> bool:
        key = entity_ref(row)
        if not key or key in seen_entities:
            return False
        seen_entities.add(key)
        selected.append(row)
        return True

    for row in rows:
        if entity_validation_titles(row, validation_index):
            add(row)
        if len(selected) >= limit:
            return selected[:limit]

    for row in rows:
        alert_types = extract_alert_types(row)
        if not alert_types:
            continue
        if any(alert_type not in covered_practices for alert_type in alert_types):
            if add(row):
                covered_practices.update(alert_types)
        if len(selected) >= limit:
            return selected[:limit]

    for row in rows:
        add(row)
        if len(selected) >= limit:
            return selected[:limit]

    return selected[:limit]


def summarize_graph(api_base: str, company_document_id: str) -> dict[str, Any] | None:
    try:
        payload = fetch_json(
            api_base,
            f"/api/v1/public/graph/company/{quote(company_document_id)}?depth=2",
        )
    except RuntimeError:
        return None

    nodes = payload.get("nodes") or []
    edges = payload.get("edges") or []
    node_type_counts = Counter(str(node.get("type") or "unknown") for node in nodes)
    edge_type_counts = Counter(str(edge.get("type") or "unknown") for edge in edges)
    connected_names = dedupe_strings(
        [
            str(node.get("label") or "").strip()
            for node in nodes
            if str(node.get("document_id") or "") != company_document_id
        ]
    )[:6]

    return {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "node_types": [
            {"type": node_type, "count": count}
            for node_type, count in node_type_counts.most_common(4)
        ],
        "edge_types": [
            {"type": edge_type, "count": count}
            for edge_type, count in edge_type_counts.most_common(4)
        ],
        "connected_names": connected_names,
    }


def summarize_company_row(company: dict[str, Any]) -> dict[str, Any]:
    connected_names = dedupe_strings(
        [str(name).strip() for name in (company.get("official_names") or []) if str(name).strip()]
    )[:6]
    edge_types: list[dict[str, Any]] = []
    if int(company.get("contract_count") or 0) > 0:
        edge_types.append({"type": "CONTRATOU", "count": int(company.get("contract_count") or 0)})
    if int(company.get("sanction_count") or 0) > 0:
        edge_types.append({"type": "SANCIONADA", "count": int(company.get("sanction_count") or 0)})
    if int(company.get("official_officer_count") or 0) > 0:
        edge_types.append({"type": "OFFICER_OF", "count": int(company.get("official_officer_count") or 0)})
    if int(company.get("interadmin_agreement_count") or 0) > 0:
        edge_types.append(
            {"type": "CELEBRO_CONVENIO_INTERADMIN", "count": int(company.get("interadmin_agreement_count") or 0)}
        )
    return {
        "node_count": 1
        + min(int(company.get("official_officer_count") or 0), 6)
        + sum(
            1
            for value in (
                company.get("contract_count"),
                company.get("sanction_count"),
                company.get("interadmin_agreement_count"),
            )
            if int(value or 0) > 0
        ),
        "edge_count": sum(item["count"] > 0 for item in edge_types) + len(connected_names),
        "node_types": [
            {"type": "company", "count": 1},
            {"type": "person", "count": min(int(company.get("official_officer_count") or 0), 6)},
        ],
        "edge_types": edge_types[:4],
        "connected_names": connected_names,
    }


def _prune_graph_payload(payload: dict[str, Any]) -> dict[str, Any]:
    nodes = list(payload.get("nodes") or [])
    edges = list(payload.get("edges") or [])
    center_id = str(payload.get("center_id") or "")

    if len(nodes) <= DETAIL_GRAPH_NODE_LIMIT and len(edges) <= DETAIL_GRAPH_EDGE_LIMIT:
        return {
            "center_id": center_id,
            "nodes": nodes,
            "edges": edges,
        }

    degree = Counter()
    center_neighbors: set[str] = set()
    for edge in edges:
        source = str(edge.get("source") or "")
        target = str(edge.get("target") or "")
        degree[source] += 1
        degree[target] += 1
        if source == center_id and target:
            center_neighbors.add(target)
        if target == center_id and source:
            center_neighbors.add(source)

    nodes_by_id = {str(node.get("id") or ""): node for node in nodes}
    ordered_node_ids = sorted(
        [node_id for node_id in nodes_by_id if node_id],
        key=lambda node_id: (
            1 if node_id == center_id else 0,
            1
            if node_id in center_neighbors
            and str(nodes_by_id[node_id].get("type") or "") != "education"
            else 0,
            1 if node_id in center_neighbors else 0,
            1 if str(nodes_by_id[node_id].get("type") or "") != "education" else 0,
            degree[node_id],
            str(nodes_by_id[node_id].get("label") or ""),
        ),
        reverse=True,
    )

    selected_ids: set[str] = set()
    node_type_counts: Counter[str] = Counter()
    for node_id in ordered_node_ids:
        node_type = str(nodes_by_id[node_id].get("type") or "")
        type_limit = DETAIL_GRAPH_NODE_TYPE_LIMITS.get(node_type)
        if (
            node_id != center_id
            and type_limit is not None
            and node_type_counts[node_type] >= type_limit
        ):
            continue
        selected_ids.add(node_id)
        node_type_counts[node_type] += 1
        if len(selected_ids) >= DETAIL_GRAPH_NODE_LIMIT:
            break

    candidate_edges = [
        edge
        for edge in edges
        if str(edge.get("source") or "") in selected_ids and str(edge.get("target") or "") in selected_ids
    ]
    candidate_edges.sort(
        key=lambda edge: (
            1
            if center_id in {str(edge.get("source") or ""), str(edge.get("target") or "")}
            else 0,
            1 if str(edge.get("type") or "") != "MANTIENE_A" else 0,
            float(
                edge.get("properties", {}).get("total_value")
                or edge.get("properties", {}).get("value")
                or 0.0
            ),
            float(edge.get("confidence") or 0.0),
            str(edge.get("type") or ""),
        ),
        reverse=True,
    )
    kept_edges: list[dict[str, Any]] = []
    edge_type_counts: Counter[str] = Counter()
    for edge in candidate_edges:
        edge_type = str(edge.get("type") or "")
        edge_limit = DETAIL_GRAPH_EDGE_TYPE_LIMITS.get(edge_type)
        if edge_limit is not None and edge_type_counts[edge_type] >= edge_limit:
            continue
        kept_edges.append(edge)
        edge_type_counts[edge_type] += 1
        if len(kept_edges) >= DETAIL_GRAPH_EDGE_LIMIT:
            break

    used_node_ids = {center_id}
    for edge in kept_edges:
        used_node_ids.add(str(edge.get("source") or ""))
        used_node_ids.add(str(edge.get("target") or ""))

    kept_nodes = [
        nodes_by_id[node_id]
        for node_id in ordered_node_ids
        if node_id in used_node_ids
    ]

    return {
        "center_id": center_id,
        "nodes": kept_nodes,
        "edges": kept_edges,
    }


def fetch_graph_payload(api_base: str, entity_id: str, depth: int = DETAIL_GRAPH_DEPTH) -> dict[str, Any] | None:
    try:
        payload = fetch_json(
            api_base,
            f"/api/v1/graph/{quote(entity_id)}?depth={depth}",
        )
    except RuntimeError:
        return None
    return _prune_graph_payload(payload)


def fetch_full_graph_payload(api_base: str, entity_id: str, depth: int = DETAIL_GRAPH_DEPTH) -> dict[str, Any] | None:
    try:
        return fetch_json(
            api_base,
            f"/api/v1/graph/{quote(entity_id)}?depth={depth}",
        )
    except RuntimeError:
        return None


def summarize_pattern(pattern: dict[str, Any]) -> dict[str, Any]:
    data = pattern.get("data") or {}
    metric_chips: list[str] = []
    for key, label in METRIC_LABELS.items():
        if key in IGNORED_PATTERN_KEYS:
            continue
        value = data.get(key)
        if isinstance(value, (int, float)) and float(value) > 0:
            if "value" in key or "total" in key or key == "amount_total":
                metric_chips.append(f"{label}: {compact_money(value)}")
            else:
                metric_chips.append(f"{label}: {int(value) if float(value).is_integer() else round(float(value), 1)}")
        if len(metric_chips) >= 3:
            break

    return {
        "pattern_id": pattern.get("pattern_id"),
        "pattern_name": pattern.get("pattern_name"),
        "description": pattern.get("description"),
        "metric_chips": metric_chips,
    }


def build_company_highlights(company: dict[str, Any], patterns: list[dict[str, Any]]) -> list[str]:
    highlights = [
        f"{int(company.get('contract_count') or 0)} contratos por {compact_money(company.get('contract_value') or 0)}",
    ]
    if company.get("official_officer_count"):
        highlights.append(
            f"{int(company['official_officer_count'])} directivo(s) con cargo público activo"
        )
    if company.get("sanction_count"):
        highlights.append(f"{int(company['sanction_count'])} antecedente(s) sancionatorios")
    if company.get("sanctioned_still_receiving_contract_count"):
        highlights.append(
            f"{int(company['sanctioned_still_receiving_contract_count'])} contrato(s) dentro de ventana sancionatoria"
        )
    if company.get("execution_gap_contract_count"):
        highlights.append(
            f"{int(company['execution_gap_contract_count'])} contrato(s) con brecha de ejecución"
        )
    if company.get("split_contract_group_count"):
        highlights.append(
            f"{int(company['split_contract_group_count'])} grupo(s) de contratos bajo umbral"
        )
    if company.get("capacity_mismatch_contract_count"):
        highlights.append(
            f"{int(company['capacity_mismatch_contract_count'])} contrato(s) por encima de la capacidad reportada"
        )

    for pattern in patterns:
        for chip in summarize_pattern(pattern)["metric_chips"]:
            if chip not in highlights:
                highlights.append(chip)
        if len(highlights) >= 6:
            break

    return highlights[:6]


def build_person_highlights(person: dict[str, Any]) -> list[str]:
    highlights: list[str] = []
    if person.get("official_case_bulletin_count"):
        highlights.append(
            f"{int(person['official_case_bulletin_count'])} boletín(es) oficiales"
        )
    if person.get("person_sanction_count"):
        highlights.append(
            f"{int(person['person_sanction_count'])} sanción(es) oficiales registradas"
        )
    if person.get("candidacy_count"):
        highlights.append(f"{int(person['candidacy_count'])} candidatura(s) registradas")
    if person.get("donation_count"):
        highlights.append(f"{int(person['donation_count'])} donación(es) reportadas")
    if person.get("supplier_contract_count"):
        highlights.append(
            f"{int(person['supplier_contract_count'])} contrato(s) como proveedor por {compact_money(person.get('supplier_contract_value') or 0)}"
        )
    if person.get("office_count"):
        highlights.append(f"{int(person['office_count'])} cargo(s) públicos vinculados")
    if person.get("conflict_disclosure_count"):
        highlights.append(
            f"{int(person['conflict_disclosure_count'])} declaración(es) de conflicto"
        )
    if person.get("disclosure_reference_count"):
        highlights.append(
            f"{int(person['disclosure_reference_count'])} referencia(s) societarias en declaraciones"
        )
    return highlights[:6]


def extract_public_sources(validation_cases: list[dict[str, Any]]) -> list[str]:
    sources: list[str] = []
    for case in validation_cases:
        for url in case.get("public_sources") or []:
            sources.append(str(url))
    return dedupe_strings(sources)


def build_company_feature(
    api_base: str,
    company: dict[str, Any],
    validation_index: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    document_id = str(company.get("document_id") or "").strip()
    if not document_id:
        return None

    validation_cases = validation_index.get(normalize_ref(document_id), [])
    patterns: list[dict[str, Any]] = []
    graph_summary = summarize_company_row(company)

    practice_labels = dedupe_strings(
        [format_signal_label(str(alert.get("alert_type") or "")) for alert in company.get("alerts") or []]
        + [format_signal_label(str(pattern.get("pattern_id") or "")) for pattern in patterns]
    )[:6]

    return {
        "entity_type": "company",
        "entity_id": company.get("entity_id"),
        "document_id": document_id,
        "name": company.get("name"),
        "risk_score": company.get("suspicion_score"),
        "signal_types": company.get("signal_types"),
        "primary_reason": (
            (company.get("alerts") or [{}])[0].get("reason_text")
            if company.get("alerts")
            else None
        ),
        "practice_labels": practice_labels,
        "highlights": build_company_highlights(company, patterns),
        "alerts": [
            {
                "alert_type": alert.get("alert_type"),
                "label": format_signal_label(str(alert.get("alert_type") or "")),
                "reason_text": alert.get("reason_text"),
                "confidence_tier": alert.get("confidence_tier"),
                "severity_score": alert.get("severity_score"),
                "source_list": alert.get("source_list") or [],
            }
            for alert in (company.get("alerts") or [])[:3]
        ],
        "patterns": [summarize_pattern(pattern) for pattern in patterns[:4]],
        "graph_summary": graph_summary,
        "matched_validation_titles": [case.get("title") for case in validation_cases],
        "public_sources": extract_public_sources(validation_cases),
    }


def build_person_feature(
    person: dict[str, Any],
    validation_index: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    document_id = str(person.get("document_id") or "").strip()
    case_person_id = str(person.get("case_person_id") or "").strip()
    validation_key = normalize_ref(document_id or case_person_id)
    validation_cases = validation_index.get(validation_key, []) if validation_key else []
    practice_labels = dedupe_strings(
        [format_signal_label(str(alert.get("alert_type") or "")) for alert in person.get("alerts") or []]
    )[:6]
    return {
        "entity_type": "person",
        "entity_id": person.get("entity_id"),
        "document_id": document_id or None,
        "case_person_id": case_person_id or None,
        "name": person.get("name"),
        "risk_score": person.get("suspicion_score"),
        "signal_types": person.get("signal_types"),
        "primary_reason": (
            (person.get("alerts") or [{}])[0].get("reason_text")
            if person.get("alerts")
            else None
        ),
        "practice_labels": practice_labels,
        "highlights": build_person_highlights(person),
        "alerts": [
            {
                "alert_type": alert.get("alert_type"),
                "label": format_signal_label(str(alert.get("alert_type") or "")),
                "reason_text": alert.get("reason_text"),
                "confidence_tier": alert.get("confidence_tier"),
                "severity_score": alert.get("severity_score"),
                "source_list": alert.get("source_list") or [],
            }
            for alert in (person.get("alerts") or [])[:3]
        ],
        "matched_validation_titles": [case.get("title") for case in validation_cases],
        "public_sources": extract_public_sources(validation_cases),
    }


def safe_case_filename(entity_type: str, entity_id: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", f"{entity_type}-{entity_id}").strip("-")
    return f"{slug or 'case'}.json"


def build_materialized_case(
    entity_type: str,
    row: dict[str, Any],
    validation_index: dict[str, list[dict[str, Any]]],
    graph: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if graph is None:
        return None

    document_id = str(row.get("document_id") or "").strip() or None
    case_person_id = str(row.get("case_person_id") or "").strip() or None
    validation_cases = validation_index.get(
        normalize_ref(document_id or case_person_id or row.get("entity_id") or row.get("name"))
    ) or []
    subtitle = "Empresa" if entity_type == "company" else "Persona"
    if document_id:
        subtitle = f"{subtitle} · {document_id}"

    return {
        "id": f"{entity_type}:{row.get('entity_id')}",
        "title": row.get("name"),
        "subtitle": subtitle,
        "summary": (
            (row.get("alerts") or [{}])[0].get("reason_text")
            if row.get("alerts")
            else None
        ),
        "tags": dedupe_strings(
            [
                format_signal_label(str(alert.get("alert_type") or ""))
                for alert in row.get("alerts") or []
            ]
        )[:6],
        "public_sources": extract_public_sources(validation_cases),
        "graph": graph,
    }


def _maybe_add_node(
    nodes: list[dict[str, Any]],
    node_ids: set[str],
    *,
    node_id: str,
    label: str,
    node_type: str,
    document_id: str | None = None,
    properties: dict[str, Any] | None = None,
) -> None:
    if node_id in node_ids:
        return
    node_ids.add(node_id)
    nodes.append(
        build_graph_node(
            node_id,
            label,
            node_type,
            document_id=document_id,
            properties=properties or {},
        )
    )


def build_watchlist_evidence_graph(entity_type: str, row: dict[str, Any]) -> dict[str, Any]:
    entity_id = str(row.get("entity_id") or "").strip()
    center_id = f"{entity_type}:{entity_id}"
    label = str(row.get("name") or entity_id or entity_type).strip() or entity_type.title()
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    node_ids: set[str] = set()

    center_props = {
        "risk_score": row.get("suspicion_score") or 0,
        "signal_types": row.get("signal_types") or 0,
    }
    if row.get("case_person_id") not in (None, ""):
        center_props["case_person_id"] = row.get("case_person_id")
    if row.get("contract_count") not in (None, ""):
        center_props["contract_count"] = row.get("contract_count")
    if row.get("contract_value") not in (None, ""):
        center_props["contract_value"] = row.get("contract_value")
    _maybe_add_node(
        nodes,
        node_ids,
        node_id=center_id,
        label=label,
        node_type=entity_type,
        document_id=str(row.get("document_id") or "").strip() or None,
        properties=center_props,
    )

    def add_aggregate(
        key: str,
        agg_label: str,
        node_type: str,
        edge_type: str,
        properties: dict[str, Any] | None = None,
        confidence: float = 1.0,
    ) -> None:
        aggregate_id = f"{center_id}:{key}"
        _maybe_add_node(
            nodes,
            node_ids,
            node_id=aggregate_id,
            label=agg_label,
            node_type=node_type,
            properties=properties or {},
        )
        if entity_type == "company":
            edges.append(build_graph_edge(aggregate_id, center_id, edge_type, confidence=confidence, properties=properties))
        else:
            edges.append(build_graph_edge(center_id, aggregate_id, edge_type, confidence=confidence, properties=properties))

    if entity_type == "company":
        for index, name in enumerate(list(row.get("official_names") or [])[:6], start=1):
            person_id = f"{center_id}:official:{index}"
            _maybe_add_node(
                nodes,
                node_ids,
                node_id=person_id,
                label=str(name),
                node_type="person",
            )
            edges.append(
                build_graph_edge(
                    person_id,
                    center_id,
                    "OFFICER_OF",
                    properties={"match_reason": "watchlist_overlap"},
                )
            )

        if int(row.get("sanction_count") or 0) > 0:
            add_aggregate(
                "sanctions",
                f"Sanciones oficiales ({int(row.get('sanction_count') or 0)})",
                "sanction",
                "SANCIONADA",
                properties={"sanction_count": int(row.get("sanction_count") or 0)},
            )
        if int(row.get("contract_count") or 0) > 0:
            add_aggregate(
                "contracts",
                f"Contratos detectados ({int(row.get('contract_count') or 0)})",
                "contract",
                "CONTRATOU",
                properties={
                    "contract_count": int(row.get("contract_count") or 0),
                    "total_value": float(row.get("contract_value") or 0.0),
                },
            )
        if int(row.get("sanctioned_still_receiving_contract_count") or 0) > 0:
            add_aggregate(
                "sanction-window",
                f"Contratos en ventana sancionatoria ({int(row.get('sanctioned_still_receiving_contract_count') or 0)})",
                "contract",
                "CONTRATOU",
                properties={
                    "contract_count": int(row.get("sanctioned_still_receiving_contract_count") or 0),
                    "total_value": float(row.get("sanctioned_still_receiving_total") or 0.0),
                },
                confidence=0.95,
            )
        if int(row.get("interadmin_agreement_count") or 0) > 0:
            add_aggregate(
                "interadmin",
                f"Convenios interadministrativos ({int(row.get('interadmin_agreement_count') or 0)})",
                "convenio",
                "CELEBRO_CONVENIO_INTERADMIN",
                properties={
                    "agreement_count": int(row.get("interadmin_agreement_count") or 0),
                    "total_value": float(row.get("interadmin_total") or 0.0),
                },
            )
        if int(row.get("execution_gap_contract_count") or 0) > 0:
            add_aggregate(
                "execution-gap",
                f"Brecha de ejecución ({int(row.get('execution_gap_contract_count') or 0)})",
                "contract",
                "CONTRATOU",
                properties={
                    "contract_count": int(row.get("execution_gap_contract_count") or 0),
                    "invoice_total_value": float(row.get("execution_gap_invoice_total") or 0.0),
                },
            )
        if int(row.get("commitment_gap_contract_count") or 0) > 0:
            add_aggregate(
                "commitment-gap",
                f"Brecha presupuestal ({int(row.get('commitment_gap_contract_count') or 0)})",
                "contract",
                "CONTRATOU",
                properties={
                    "contract_count": int(row.get("commitment_gap_contract_count") or 0),
                    "total_value": float(row.get("commitment_gap_total") or 0.0),
                },
            )
        if int(row.get("split_contract_group_count") or 0) > 0:
            add_aggregate(
                "split-groups",
                f"Paquetes bajo umbral ({int(row.get('split_contract_group_count') or 0)})",
                "bid",
                "ADJUDICOU_A",
                properties={
                    "group_count": int(row.get("split_contract_group_count") or 0),
                    "total_value": float(row.get("split_contract_total") or 0.0),
                },
            )
    else:
        if int(row.get("person_sanction_count") or 0) > 0:
            add_aggregate(
                "sanctions",
                f"Sanciones oficiales ({int(row.get('person_sanction_count') or 0)})",
                "sanction",
                "SANCIONADA",
                properties={"sanction_count": int(row.get("person_sanction_count") or 0)},
            )
        if int(row.get("office_count") or 0) > 0:
            add_aggregate(
                "public-offices",
                f"Cargos públicos ({int(row.get('office_count') or 0)})",
                "public_office",
                "RECIBIO_SALARIO",
                properties={"office_count": int(row.get("office_count") or 0)},
            )
        if int(row.get("candidacy_count") or 0) > 0:
            add_aggregate(
                "candidacies",
                f"Candidaturas ({int(row.get('candidacy_count') or 0)})",
                "election",
                "CANDIDATO_EM",
                properties={"candidacy_count": int(row.get("candidacy_count") or 0)},
            )
        if int(row.get("donation_count") or 0) > 0:
            add_aggregate(
                "donations",
                f"Donaciones ({int(row.get('donation_count') or 0)})",
                "election",
                "DONO_A",
                properties={"donation_count": int(row.get("donation_count") or 0)},
            )
        if int(row.get("supplier_contract_count") or 0) > 0:
            add_aggregate(
                "supplier-contracts",
                f"Contratos como proveedora/o ({int(row.get('supplier_contract_count') or 0)})",
                "contract",
                "CONTRATOU",
                properties={
                    "contract_count": int(row.get("supplier_contract_count") or 0),
                    "total_value": float(row.get("supplier_contract_value") or 0.0),
                },
            )
        if int(row.get("linked_supplier_company_count") or 0) > 0:
            add_aggregate(
                "linked-companies",
                f"Empresas proveedoras vinculadas ({int(row.get('linked_supplier_company_count') or 0)})",
                "company",
                "OFFICER_OF",
                properties={"company_count": int(row.get("linked_supplier_company_count") or 0)},
            )
        if int(row.get("conflict_disclosure_count") or 0) > 0:
            add_aggregate(
                "conflicts",
                f"Declaraciones de conflicto ({int(row.get('conflict_disclosure_count') or 0)})",
                "finance",
                "DECLARO_FINANZAS",
                properties={"conflict_disclosure_count": int(row.get("conflict_disclosure_count") or 0)},
            )
        if int(row.get("disclosure_reference_count") or 0) > 0:
            add_aggregate(
                "references",
                f"Referencias societarias ({int(row.get('disclosure_reference_count') or 0)})",
                "company",
                "REFERENTE_A",
                properties={"reference_count": int(row.get("disclosure_reference_count") or 0)},
            )
        if int(row.get("official_case_bulletin_count") or 0) > 0:
            add_aggregate(
                "official-bulletins",
                f"Boletines oficiales ({int(row.get('official_case_bulletin_count') or 0)})",
                "inquiry",
                "REFERENTE_A",
                properties={
                    "bulletin_count": int(row.get("official_case_bulletin_count") or 0),
                    "titles": list(row.get("official_case_bulletin_titles") or []),
                },
                confidence=0.98,
            )
        if int(row.get("payment_supervision_count") or 0) > 0:
            add_aggregate(
                "payment-supervision",
                f"Supervisión de pagos ({int(row.get('payment_supervision_count') or 0)})",
                "contract",
                "SUPERVISA_PAGO",
                properties={"contract_count": int(row.get("payment_supervision_count") or 0)},
            )

    return {
        "center_id": center_id,
        "nodes": nodes,
        "edges": edges,
    }


def build_practice_group_item(
    entity_type: str,
    row: dict[str, Any],
    validation_index: dict[str, list[dict[str, Any]]],
    reason_text: str | None,
) -> dict[str, Any]:
    return {
        "entity_type": entity_type,
        "entity_id": row.get("entity_id"),
        "document_id": str(row.get("document_id") or "").strip() or None,
        "name": row.get("name"),
        "risk_score": row.get("suspicion_score") or 0,
        "reason_text": reason_text,
        "matched_validation_titles": entity_validation_titles(row, validation_index),
    }


def build_practice_groups(
    companies: list[dict[str, Any]],
    people: list[dict[str, Any]],
    validation_index: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}

    def ensure_group(label: str) -> dict[str, Any]:
        group = groups.get(label)
        if group is None:
            group = {
                "label": label,
                "companies": [],
                "people": [],
                "_company_seen": set(),
                "_person_seen": set(),
            }
            groups[label] = group
        return group

    for company in companies:
        for alert in company.get("alerts") or []:
            alert_type = str(alert.get("alert_type") or "").strip()
            if not alert_type:
                continue
            label = format_signal_label(alert_type)
            group = ensure_group(label)
            key = entity_ref(company)
            if key in group["_company_seen"]:
                continue
            group["_company_seen"].add(key)
            group["companies"].append(
                build_practice_group_item(
                    "company",
                    company,
                    validation_index,
                    str(alert.get("reason_text") or "").strip() or None,
                )
            )

    for person in people:
        for alert in person.get("alerts") or []:
            alert_type = str(alert.get("alert_type") or "").strip()
            if not alert_type:
                continue
            label = format_signal_label(alert_type)
            group = ensure_group(label)
            key = entity_ref(person)
            if key in group["_person_seen"]:
                continue
            group["_person_seen"].add(key)
            group["people"].append(
                build_practice_group_item(
                    "person",
                    person,
                    validation_index,
                    str(alert.get("reason_text") or "").strip() or None,
                )
            )

    built_groups: list[dict[str, Any]] = []
    for group in groups.values():
        companies_sorted = sorted(
            group["companies"],
            key=lambda item: (
                len(item.get("matched_validation_titles") or []),
                float(item.get("risk_score") or 0),
            ),
            reverse=True,
        )[:PRACTICE_GROUP_ITEM_LIMIT]
        people_sorted = sorted(
            group["people"],
            key=lambda item: (
                len(item.get("matched_validation_titles") or []),
                float(item.get("risk_score") or 0),
            ),
            reverse=True,
        )[:PRACTICE_GROUP_ITEM_LIMIT]
        built_groups.append(
            {
                "label": group["label"],
                "company_count": len(group["_company_seen"]),
                "person_count": len(group["_person_seen"]),
                "total_hits": len(group["_company_seen"]) + len(group["_person_seen"]),
                "validation_hits": sum(
                    1
                    for item in [*companies_sorted, *people_sorted]
                    if item.get("matched_validation_titles")
                ),
                "companies": companies_sorted,
                "people": people_sorted,
            }
        )

    built_groups.sort(
        key=lambda group: (
            int(group["validation_hits"]),
            int(group["total_hits"]),
            int(group["company_count"]),
            int(group["person_count"]),
        ),
        reverse=True,
    )
    return built_groups[:PRACTICE_GROUP_LIMIT]


def should_feature_person(person: dict[str, Any]) -> bool:
    alert_types = {
        str(alert.get("alert_type") or "").strip()
        for alert in person.get("alerts") or []
        if str(alert.get("alert_type") or "").strip()
    }
    return bool(
        (person.get("candidacy_count") or 0) > 0
        or ((person.get("donation_count") or 0) > 0 and (person.get("supplier_contract_count") or 0) > 0)
        or (person.get("office_count") or 0) > 0 and (person.get("supplier_contract_count") or 0) > 0
        or "payment_supervision_risk_stack" in alert_types
        or "sanctioned_person_exposure_stack" in alert_types
    )


def build_practice_summary(practice_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"label": group["label"], "count": group["total_hits"]}
        for group in practice_groups[:8]
    ]


def build_evidence_item(label: str, value: str, detail: str | None = None) -> dict[str, Any]:
    return {
        "label": label,
        "value": value,
        "detail": detail,
    }


def build_graph_node(
    node_id: str,
    label: str,
    node_type: str,
    *,
    document_id: str | None = None,
    properties: dict[str, Any] | None = None,
    source_name: str | None = None,
    source_url: str | None = None,
) -> dict[str, Any]:
    node: dict[str, Any] = {
        "id": node_id,
        "label": label,
        "type": node_type,
        "properties": properties or {},
        "sources": [],
        "is_pep": False,
        "exposure_tier": "public_safe",
    }
    if document_id:
        node["document_id"] = document_id
    if source_name or source_url:
        node["sources"] = [
            {
                "database": source_name or "public_document",
                "record_id": source_url or None,
                "extracted_at": None,
            }
        ]
    return node


def build_graph_edge(
    source: str,
    target: str,
    edge_type: str,
    *,
    confidence: float = 1.0,
    properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "source": source,
        "target": target,
        "type": edge_type,
        "confidence": confidence,
        "properties": properties or {},
    }


def merge_graph_payload(
    base_graph: dict[str, Any],
    *,
    extra_nodes: list[dict[str, Any]] | None = None,
    extra_edges: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    nodes_by_id = {
        str(node.get("id") or ""): node
        for node in (base_graph.get("nodes") or [])
        if str(node.get("id") or "")
    }
    for node in extra_nodes or []:
        node_id = str(node.get("id") or "")
        if node_id:
            nodes_by_id[node_id] = node

    merged_edges: list[dict[str, Any]] = []
    seen_edges: set[tuple[str, str, str, str]] = set()
    for edge in [*(base_graph.get("edges") or []), *(extra_edges or [])]:
        source = str(edge.get("source") or "")
        target = str(edge.get("target") or "")
        edge_type = str(edge.get("type") or "")
        props = edge.get("properties") or {}
        edge_key = (
            source,
            target,
            edge_type,
            str(props.get("match_reason") or props.get("role") or props.get("evidence_refs") or ""),
        )
        if source and target and edge_type and edge_key not in seen_edges:
            seen_edges.add(edge_key)
            merged_edges.append(edge)

    return {
        "center_id": str(base_graph.get("center_id") or ""),
        "nodes": list(nodes_by_id.values()),
        "edges": merged_edges,
    }


def build_metrics_evidence(
    metrics: dict[str, Any],
    keys: list[str],
) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for key in keys:
        value = metrics.get(key)
        if value in (None, "", [], 0, 0.0):
            continue
        label = METRIC_LABELS.get(key, key.replace("_", " "))
        if isinstance(value, (int, float)):
            if ("value" in key or "total" in key) and not (
                key == "archive_document_total" or key.startswith("archive_") and key.endswith("_document_total")
            ):
                rendered = compact_money(value)
            else:
                rendered = str(int(value) if float(value).is_integer() else round(float(value), 1))
        elif isinstance(value, list):
            rendered = ", ".join(str(item) for item in value[:3])
        else:
            rendered = str(value)
        evidence.append(build_evidence_item(label, rendered))
    return evidence


def combine_public_sources(*cases: dict[str, Any]) -> list[str]:
    return dedupe_strings(
        [
            str(url)
            for case in cases
            for url in (case.get("public_sources") or [])
            if str(url).strip()
        ]
    )


def generated_public_sources(alert_types: set[str]) -> list[str]:
    sources = [
        DATASET_URLS["secop_i_historical_processes"],
        DATASET_URLS["secop_i_resource_origins"],
    ]
    if "payment_supervision_risk_stack" in alert_types:
        sources.append(DATASET_URLS["secop_payment_plans"])
    if {"sanctioned_supplier_record", "sanctioned_still_receiving"} & alert_types:
        sources.extend(
            [
                DATASET_URLS["siri_antecedents"],
                DATASET_URLS["fiscal_responsibility"],
                DATASET_URLS["paco_sanctions"],
            ]
        )
    if {"public_official_supplier_overlap", "sensitive_public_official_supplier_overlap"} & alert_types:
        sources.extend(
            [
                DATASET_URLS["company_registry_c82u"],
                DATASET_URLS["sigep_public_servants"],
                DATASET_URLS["sigep_sensitive_positions"],
            ]
        )
    if "interadministrative_channel_stacking" in alert_types:
        sources.append(DATASET_URLS["secop_interadmin_agreements"])
    return dedupe_strings(sources)


def looks_like_public_entity(name: str) -> bool:
    normalized = (name or "").strip().upper()
    public_prefixes = (
        "MUNICIPIO",
        "DEPARTAMENTO",
        "GOBERNACION",
        "ALCALDIA",
        "BOGOTA D.C. -",
        "RISARALDA -",
        "NORTE DE SANTANDER -",
        "HOSPITAL ",
        "E.S.E.",
        "UNIVERSIDAD ",
        "INSTITUTO ",
        "SERVICIO NACIONAL",
        "EMPRESA ",
        "CAJA DE COMPENSACION",
        "FONDO FINANCIERO",
        "AGENCIA LOGISTICA",
        "RTVC",
        "POLITECNICO ",
    )
    return normalized.startswith(public_prefixes)


def summarize_company_graph(
    graph: dict[str, Any] | None,
    *,
    include_neighbor_contracts: bool = False,
) -> dict[str, Any]:
    if not graph:
        return {
            "historical_contract_count": 0,
            "historical_contract_value": 0.0,
            "historical_with_origin_count": 0,
            "historical_resource_origin_total": 0.0,
            "archive_contract_count": 0,
            "archive_document_total": 0,
            "archive_supervision_contract_count": 0,
            "archive_supervision_document_total": 0,
            "archive_payment_contract_count": 0,
            "archive_payment_document_total": 0,
            "archive_start_record_contract_count": 0,
            "archive_start_record_document_total": 0,
            "archive_resume_contract_count": 0,
            "archive_resume_document_total": 0,
            "archive_assignment_contract_count": 0,
            "archive_assignment_document_total": 0,
            "archive_report_document_total": 0,
            "archive_examples": [],
            "top_historical_buyers": [],
        }

    nodes_by_id = {str(node.get("id") or ""): node for node in graph.get("nodes") or []}
    center_id = str(graph.get("center_id") or "")
    center_neighbors: set[str] = set()
    for edge in graph.get("edges") or []:
        source = str(edge.get("source") or "")
        target = str(edge.get("target") or "")
        if source == center_id and target:
            center_neighbors.add(target)
        if target == center_id and source:
            center_neighbors.add(source)
    contract_edges = [
        edge
        for edge in graph.get("edges") or []
        if str(edge.get("type") or "") == "CONTRATOU"
        and (
            str(edge.get("source") or "") == center_id
            or str(edge.get("target") or "") == center_id
            or (
                include_neighbor_contracts
                and (
                    str(edge.get("source") or "") in center_neighbors
                    or str(edge.get("target") or "") in center_neighbors
                )
            )
        )
    ]
    historical_edges = [
        edge
        for edge in contract_edges
        if bool((edge.get("properties") or {}).get("historical"))
    ]
    buyer_counter: Counter[str] = Counter()
    historical_with_origin_count = 0
    historical_value = 0.0
    historical_origin_total = 0.0
    archive_contract_count = 0
    archive_document_total = 0
    archive_supervision_contract_count = 0
    archive_supervision_document_total = 0
    archive_payment_contract_count = 0
    archive_payment_document_total = 0
    archive_start_record_contract_count = 0
    archive_start_record_document_total = 0
    archive_resume_contract_count = 0
    archive_resume_document_total = 0
    archive_assignment_contract_count = 0
    archive_assignment_document_total = 0
    archive_report_document_total = 0
    archive_examples: list[dict[str, Any]] = []

    def normalize_archive_parts(value: Any) -> list[str]:
        if isinstance(value, list):
            return dedupe_strings([str(item).strip() for item in value if str(item).strip()])
        if isinstance(value, str):
            return dedupe_strings([part.strip() for part in value.split(",") if part.strip()])
        return []

    for edge in historical_edges:
        properties = edge.get("properties") or {}
        historical_value += float(properties.get("total_value") or 0.0)
        if float(properties.get("resource_origin_count") or 0) > 0:
            historical_with_origin_count += 1
            historical_origin_total += float(properties.get("resource_origin_total") or 0.0)
        buyer_name = str(nodes_by_id.get(str(edge.get("source") or ""), {}).get("label") or "").strip()
        if buyer_name:
            buyer_counter[buyer_name] += 1

    for edge in contract_edges:
        properties = edge.get("properties") or {}
        archive_document_count = int(properties.get("archive_document_count") or 0)
        if archive_document_count <= 0:
            continue
        archive_contract_count += 1
        archive_document_total += archive_document_count
        archive_names = normalize_archive_parts(properties.get("archive_document_names"))
        archive_refs = normalize_archive_parts(properties.get("archive_document_refs"))
        normalized_names = [str(name).strip().lower() for name in archive_names if str(name).strip()]
        archive_supervision_document_count = int(properties.get("archive_supervision_document_count") or 0)
        archive_payment_document_count = int(properties.get("archive_payment_document_count") or 0)
        archive_assignment_document_count = int(properties.get("archive_assignment_document_count") or 0)
        archive_start_record_document_count = int(properties.get("archive_start_record_document_count") or 0)
        archive_resume_document_count = int(properties.get("archive_resume_document_count") or 0)
        archive_report_document_count = int(properties.get("archive_report_document_count") or 0)
        archive_supervision_document_total += archive_supervision_document_count
        archive_payment_document_total += archive_payment_document_count
        archive_assignment_document_total += archive_assignment_document_count
        archive_start_record_document_total += archive_start_record_document_count
        archive_resume_document_total += archive_resume_document_count
        archive_report_document_total += archive_report_document_count
        if archive_supervision_document_count > 0 or any("supervis" in name for name in normalized_names):
            archive_supervision_contract_count += 1
        if archive_payment_document_count > 0 or any("pago" in name for name in normalized_names):
            archive_payment_contract_count += 1
        if archive_start_record_document_count > 0 or any(
            "acta de inicio" in name or "oficio de inicio" in name for name in normalized_names
        ):
            archive_start_record_contract_count += 1
        if archive_resume_document_count > 0 or any("hoja de vida" in name for name in normalized_names):
            archive_resume_contract_count += 1
        if archive_assignment_document_count > 0 or any("designaci" in name or "delegaci" in name for name in normalized_names):
            archive_assignment_contract_count += 1
        archive_examples.append(
            {
                "buyer_name": str(nodes_by_id.get(str(edge.get("source") or ""), {}).get("label") or "").strip(),
                "supplier_name": str(nodes_by_id.get(str(edge.get("target") or ""), {}).get("label") or "").strip(),
                "total_value": float(properties.get("total_value") or 0.0),
                "archive_document_count": archive_document_count,
                "document_samples": archive_names[:4],
                "reference_samples": archive_refs[:4],
            }
        )

    archive_examples.sort(
        key=lambda item: (
            int(item.get("archive_document_count") or 0),
            float(item.get("total_value") or 0.0),
        ),
        reverse=True,
    )

    return {
        "historical_contract_count": len(historical_edges),
        "historical_contract_value": historical_value,
        "historical_with_origin_count": historical_with_origin_count,
        "historical_resource_origin_total": historical_origin_total,
        "archive_contract_count": archive_contract_count,
        "archive_document_total": archive_document_total,
        "archive_supervision_contract_count": archive_supervision_contract_count,
        "archive_supervision_document_total": archive_supervision_document_total,
        "archive_payment_contract_count": archive_payment_contract_count,
        "archive_payment_document_total": archive_payment_document_total,
        "archive_start_record_contract_count": archive_start_record_contract_count,
        "archive_start_record_document_total": archive_start_record_document_total,
        "archive_resume_contract_count": archive_resume_contract_count,
        "archive_resume_document_total": archive_resume_document_total,
        "archive_assignment_contract_count": archive_assignment_contract_count,
        "archive_assignment_document_total": archive_assignment_document_total,
        "archive_report_document_total": archive_report_document_total,
        "archive_examples": archive_examples[:3],
        "top_historical_buyers": [name for name, _count in buyer_counter.most_common(3)],
    }


def archive_priority_tuple(graph_metrics: dict[str, Any]) -> tuple[int, int, int, int, int]:
    return (
        int(graph_metrics.get("archive_supervision_contract_count") or 0),
        int(graph_metrics.get("archive_payment_contract_count") or 0),
        int(graph_metrics.get("archive_assignment_contract_count") or 0),
        int(graph_metrics.get("archive_contract_count") or 0),
        int(graph_metrics.get("archive_document_total") or 0),
    )


def format_archive_example(graph_metrics: dict[str, Any]) -> str | None:
    example = (graph_metrics.get("archive_examples") or [{}])[0]
    buyer_name = str(example.get("buyer_name") or "").strip()
    supplier_name = str(example.get("supplier_name") or "").strip()
    document_samples = list(example.get("document_samples") or [])
    if not buyer_name and not supplier_name:
        return None
    route = " -> ".join([name for name in [buyer_name, supplier_name] if name])
    detail = route
    total_value = float(example.get("total_value") or 0.0)
    if total_value > 0:
        detail += f" por {compact_money(total_value)}"
    if document_samples:
        detail += ", con soportes como " + ", ".join(document_samples[:3])
    return detail


def search_entity_id(api_base: str, query: str) -> str | None:
    try:
        payload = fetch_json(api_base, f"/api/v1/search?q={quote(query)}")
    except RuntimeError:
        return None
    results = payload.get("results") or []
    if not results:
        return None
    return str(results[0].get("id") or "").strip() or None


def build_generic_investigation_graph(
    api_base: str,
    entity_id: str,
    *,
    depth: int = DETAIL_GRAPH_DEPTH,
) -> dict[str, Any] | None:
    return fetch_graph_payload(api_base, entity_id, depth=depth)


def build_san_jose_investigation(
    api_base: str,
    case: dict[str, Any],
) -> dict[str, Any] | None:
    entity_id = str(case.get("entity_id") or "").strip()
    if not entity_id:
        return None

    graph = build_generic_investigation_graph(api_base, entity_id, depth=2)
    if not graph:
        return None

    nodes_by_id = {str(node.get("id") or ""): node for node in graph.get("nodes") or []}
    non_education_edges = [
        edge for edge in (graph.get("edges") or []) if str(edge.get("type") or "") != "MANTIENE_A"
    ]
    controller_rows = []
    agreement_rows = []
    alias_label = None
    alias_document = None

    for edge in non_education_edges:
        edge_type = str(edge.get("type") or "")
        source = nodes_by_id.get(str(edge.get("source") or ""))
        target = nodes_by_id.get(str(edge.get("target") or ""))
        if edge_type == "ADMINISTRA" and source and target:
            controller_rows.append(
                {
                    "name": source.get("label"),
                    "role": edge.get("properties", {}).get("role"),
                }
            )
        elif edge_type == "SAME_AS" and target:
            alias_label = str(target.get("label") or "").strip() or None
            alias_document = str(target.get("document_id") or "").strip() or None
        elif edge_type == "CELEBRO_CONVENIO_INTERADMIN" and source:
            props = edge.get("properties") or {}
            refs = props.get("evidence_refs") or []
            if isinstance(refs, str):
                refs = [refs]
            agreement_rows.append(
                {
                    "buyer": source.get("label"),
                    "evidence_refs": refs,
                    "total_value": float(props.get("total_value") or 0.0),
                    "first_date": props.get("first_date"),
                    "object": props.get("object"),
                }
            )

    agreement_rows.sort(key=lambda item: item["total_value"], reverse=True)
    metrics = case.get("metrics") or {}

    icaft_id = search_entity_id(api_base, "icaft")
    icaft_graph = build_generic_investigation_graph(api_base, icaft_id, depth=2) if icaft_id else None
    icaft_controllers: list[str] = []
    icaft_family_detail = ""
    if icaft_graph:
        icaft_nodes = {str(node.get("id") or ""): node for node in icaft_graph.get("nodes") or []}
        for edge in icaft_graph.get("edges") or []:
            edge_type = str(edge.get("type") or "")
            if edge_type == "ADMINISTRA":
                source = icaft_nodes.get(str(edge.get("source") or ""))
                if source:
                    icaft_controllers.append(str(source.get("label") or ""))
            elif edge_type == "POSSIBLE_FAMILY_TIE":
                props = edge.get("properties") or {}
                shared = props.get("shared_surnames") or []
                if isinstance(shared, str):
                    shared_text = shared
                else:
                    shared_text = ", ".join(str(item) for item in shared)
                icaft_family_detail = shared_text or str(props.get("match_reason") or "")

    findings: list[str] = []
    if controller_rows:
        controller_text = " · ".join(
            f"{row['name']} ({row['role']})"
            for row in controller_rows[:3]
            if row.get("name") and row.get("role")
        )
        findings.append(f"San José aparece con {len(controller_rows)} controladores institucionales cargados en MEN: {controller_text}.")
    if alias_document:
        findings.append(
            f"El grafo enlaza la institución con un alias SECOP ({alias_document}) mediante una coincidencia nominal y de prefijo numérico."
        )
    if agreement_rows:
        top_refs = []
        for agreement in agreement_rows[:2]:
            refs = agreement.get("evidence_refs") or []
            if isinstance(refs, str):
                top_refs.append(refs)
            elif isinstance(refs, list):
                top_refs.extend(str(ref) for ref in refs[:2])
        ref_text = ", ".join(dedupe_strings(top_refs)[:4])
        findings.append(
            "A través de ese alias aparecen "
            f"{len(agreement_rows)} convenios interadministrativos con "
            f"{agreement_rows[0]['buyer']} por {compact_money(metrics.get('education_procurement_total') or 0)} "
            + (f"en 2024, con referencias {ref_text}." if ref_text else "en 2024.")
        )
    if icaft_controllers:
        controller_text = " y ".join(dedupe_strings(icaft_controllers)[:2])
        detail = f" La red de ICAFT marca apellidos compartidos: {icaft_family_detail}." if icaft_family_detail else ""
        findings.append(
            f"Como línea paralela de revisión, ICAFT aparece en la misma carga educativa con {controller_text} como controladores.{detail}"
        )

    san_jose_policy_url = (
        "https://sitio.usanjose.edu.co/wp-content/uploads/2024/04/Politica-de-seguridad-de-la-informacion-FESSJ.pdf"
    )
    san_jose_directory_url = "https://sitio.usanjose.edu.co/institucional-documentos-institucionales/"
    icaft_docs_url = "https://icaft.edu.co/institucional/"
    icaft_fundadores_url = "https://icaft.edu.co/wp-content/uploads/2024/06/Fundadores.pdf"
    icaft_certificate_url = "https://icaft.edu.co/wp-content/uploads/2024/06/CCIO-ICAFT.pdf"
    icaft_rut_sep_2025_url = "https://icaft.edu.co/wp-content/uploads/2025/09/RUT-ICAFT-26-SEP.pdf"
    icaft_rut_url = "https://icaft.edu.co/wp-content/uploads/2026/03/RUT-ICAFT-6.pdf"
    minedu_case_url = "https://www.mineducacion.gov.co/1780/w3-article-426421.html"
    minedu_resolution_url = "https://www.mineducacion.gov.co/1780/articles-426422_recurso_1.pdf"
    c82u_dataset_url = "https://www.datos.gov.co/d/c82u-588k"
    men_institutions_dataset_url = "https://www.datos.gov.co/d/n5yy-8nav"
    men_directors_dataset_url = "https://www.datos.gov.co/d/muyy-6yw9"
    bluhartmann_url = "https://bluhartmann.com/"
    hazte_profesional_url = "https://hazteprofesional.com/"
    hazte_profesional_wp_api_url = "https://hazteprofesional.com/wp-json/"
    hazte_profesional_page_url = "https://hazteprofesional.com/wp-json/wp/v2/pages/17714"
    staging_hazte_url = "https://staging.hazteprofesional.com/"
    staging_hazte_wp_api_url = "https://staging.hazteprofesional.com/wp-json/"
    staging_hazte_page_url = "https://staging.hazteprofesional.com/wp-json/wp/v2/pages/39"
    i_hazte_url = "https://i.hazteprofesional.com/"
    alianzas_hazte_url = "https://alianzas.hazteprofesional.com/"
    convenios_bluhartmann_url = "https://convenios.bluhartmann.com/"
    convenios_bluhartmann_wp_api_url = "https://convenios.bluhartmann.com/wp-json/"
    pagos_bluhartmann_url = "https://pagos.bluhartmann.com/"
    hazte_certificates_url = "https://crt.sh/?q=hazteprofesional.com"
    bluhartmann_certificates_url = "https://crt.sh/?q=bluhartmann.com"
    bluhartmann_behance_profile_url = "https://www.behance.net/BluHartmann"
    bluhartmann_icaft_project_url = "https://www.behance.net/gallery/220077829/GRADOS-ICAFT-BLUHARTMANN"
    bluhartmann_usj_project_url = (
        "https://www.behance.net/gallery/221107913/GRADOS-USANJOSE-NACIDOS-PARA-BRILLAR-BLUHARTMANN"
    )
    observatorio_machinery_url = (
        "https://www.universidad.edu.co/la-maquinaria-que-esta-detras-del-crecimiento-de-la-san-jose-e-icaft/"
    )
    observatorio_family_url = (
        "https://www.universidad.edu.co/congresista-denuncia-que-los-mismos-que-estan-detras-de-la-san-jose-estan-detras-de-la-icaft/"
    )

    documented_nodes: list[dict[str, Any]] = []
    documented_edges: list[dict[str, Any]] = []

    san_jose_center = str(graph.get("center_id") or "")
    icaft_center_node = None
    if icaft_graph:
        icaft_center_id = str(icaft_graph.get("center_id") or "")
        icaft_center_node = next(
            (
                node
                for node in (icaft_graph.get("nodes") or [])
                if str(node.get("id") or "") == icaft_center_id
            ),
            None,
        )
        if icaft_center_node:
            documented_nodes.append(icaft_center_node)
    else:
        icaft_center_id = ""

    stefanny_node_id = "documented:person:leidy-stefanny-camacho-galindo"
    luis_node_id = "documented:person:luis-carlos-gutierrez-martinez"
    mauricio_node_id = "documented:person:9977051"
    andres_node_id = "documented:person:1018427145"
    sonia_node_id = "documented:person:52909556"
    clara_node_id = "documented:person:51618318"
    mauricio_florez_nino_node_id = "documented:person:4222003"
    david_reyes_devia_node_id = "documented:person:1030614364"
    eloira_node_id = "documented:person:30664149"
    carlos_adolfo_node_id = "documented:person:79877993"
    business_labs_node_id = "documented:company:901498512"
    lc_gutierrez_company_node_id = "documented:company:901904991"
    bissna_company_node_id = "documented:company:901724535"
    bluhartmann_brand_node_id = "documented:brand:bluhartmann"
    hazte_profesional_node_id = "documented:site:hazteprofesional"
    staging_hazte_node_id = "documented:site:staging-hazteprofesional"
    i_hazte_node_id = "documented:site:i-hazteprofesional"
    alianzas_hazte_node_id = "documented:site:alianzas-hazteprofesional"
    convenios_bluhartmann_node_id = "documented:site:convenios-bluhartmann"
    pagos_bluhartmann_node_id = "documented:site:pagos-bluhartmann"

    andres_registry_companies = [
        ("BUSINESS LABS UNITED S. A. S.", "901498512", "20210702"),
        ("INNOVATION QUALITY SUPPORT S.A.S.", "901514596", "20210825"),
        ("SOLUCIONES EN TECNOLOGIA E INGENIERIA SAS", "900713449", "20140319"),
        ("BENSON&CLAIRE S.A.S", "901955154", "20250520"),
    ]
    mauricio_registry_companies = [
        ("JOY 1 SAS", "901876209", "20241002"),
        ("SOCIEDAD DE INVERSIONES INMOBILIARIAS COGOLLO S.A.S.", "901926337", "20250312"),
        ("SOCIEDAD DE INVERSIONES INMOBILIARIAS MARTINEZ & CIA SAS", "901925998", "20250311"),
    ]

    documented_nodes.extend(
        [
            build_graph_node(
                stefanny_node_id,
                "LEIDY STEFANNY CAMACHO GALINDO",
                "person",
                properties={
                    "name": "LEIDY STEFANNY CAMACHO GALINDO",
                    "role": "Vicerrectora Académica",
                    "evidence_tier": "registro_oficial",
                },
                source_name="san_jose_documentos_oficiales",
                source_url=san_jose_directory_url,
            ),
            build_graph_node(
                luis_node_id,
                "LUIS CARLOS GUTIERREZ MARTINEZ",
                "person",
                properties={
                    "name": "LUIS CARLOS GUTIERREZ MARTINEZ",
                    "role": "Secretario General / Director de Planeación",
                    "evidence_tier": "registro_oficial",
                },
                source_name="san_jose_politica_seguridad_2024",
                source_url=san_jose_policy_url,
            ),
            build_graph_node(
                mauricio_node_id,
                "MAURICIO GUEVARA MARIN",
                "person",
                document_id="9977051",
                properties={
                    "name": "MAURICIO GUEVARA MARIN",
                    "role": "Representante legal documentado en ICAFT",
                    "evidence_tier": "registro_oficial",
                },
                source_name="icaft_certificado_oficial_2024",
                source_url=icaft_certificate_url,
            ),
            build_graph_node(
                andres_node_id,
                "ANDRES DAVID MENDEZ FLOREZ",
                "person",
                document_id="1018427145",
                properties={
                    "name": "ANDRES DAVID MENDEZ FLOREZ",
                    "role": "Representante legal en registro mercantil abierto",
                    "evidence_tier": "registro_oficial",
                },
                source_name="registro_mercantil_abierto_c82u_588k",
                source_url=c82u_dataset_url,
            ),
            build_graph_node(
                sonia_node_id,
                "SONIA ISABEL MENDEZ FLOREZ",
                "person",
                document_id="52909556",
                properties={
                    "name": "SONIA ISABEL MENDEZ FLOREZ",
                    "role": "Socia / miembro listada en RUT ICAFT",
                    "fecha_ingreso": "20240324",
                    "evidence_tier": "registro_oficial",
                },
                source_name="icaft_rut_publico_2026",
                source_url=icaft_rut_url,
            ),
            build_graph_node(
                clara_node_id,
                "CLARA INES GALINDO DIAZ",
                "person",
                document_id="51618318",
                properties={
                    "name": "CLARA INES GALINDO DIAZ",
                    "role": "Socia / miembro listada en RUT ICAFT",
                    "fecha_ingreso": "20240814",
                    "evidence_tier": "registro_oficial",
                },
                source_name="icaft_rut_publico_2026",
                source_url=icaft_rut_url,
            ),
            build_graph_node(
                mauricio_florez_nino_node_id,
                "MAURICIO FLOREZ NIÑO",
                "person",
                document_id="4222003",
                properties={
                    "name": "MAURICIO FLOREZ NIÑO",
                    "role": "Socio / miembro listado en RUT ICAFT",
                    "fecha_ingreso": "20240814",
                    "evidence_tier": "registro_oficial",
                },
                source_name="icaft_rut_publico_2026",
                source_url=icaft_rut_url,
            ),
            build_graph_node(
                david_reyes_devia_node_id,
                "DAVID FERNANDO REYES DEVIA",
                "person",
                document_id="1030614364",
                properties={
                    "name": "DAVID FERNANDO REYES DEVIA",
                    "role": "Socio / miembro listado en RUT ICAFT 2025-09-26",
                    "fecha_ingreso": "20240814",
                    "evidence_tier": "registro_oficial",
                },
                source_name="icaft_rut_publico_2025_09_26",
                source_url=icaft_rut_sep_2025_url,
            ),
            build_graph_node(
                eloira_node_id,
                "ELOIRA MARGOTH COGOLLO CARO",
                "person",
                document_id="30664149",
                properties={
                    "name": "ELOIRA MARGOTH COGOLLO CARO",
                    "role": "Representante legal en registro mercantil abierto",
                    "evidence_tier": "registro_oficial",
                },
                source_name="registro_mercantil_abierto_c82u_588k",
                source_url=c82u_dataset_url,
            ),
            build_graph_node(
                carlos_adolfo_node_id,
                "CARLOS ADOLFO MENDEZ FLOREZ",
                "person",
                document_id="79877993",
                properties={
                    "name": "CARLOS ADOLFO MENDEZ FLOREZ",
                    "role": "Representante legal en registro mercantil abierto",
                    "evidence_tier": "registro_oficial",
                },
                source_name="registro_mercantil_abierto_c82u_588k",
                source_url=c82u_dataset_url,
            ),
            build_graph_node(
                business_labs_node_id,
                "BUSINESS LABS UNITED S. A. S.",
                "company",
                document_id="901498512",
                properties={
                    "name": "BUSINESS LABS UNITED S. A. S.",
                    "razon_social": "BUSINESS LABS UNITED S. A. S.",
                    "nit": "901498512",
                    "digito_verificacion": "5",
                    "fecha_matricula": "20210702",
                    "ultimo_ano_renovado": "2025",
                    "estado_matricula": "ACTIVA",
                    "evidence_tier": "registro_oficial",
                },
                source_name="registro_mercantil_abierto_c82u_588k",
                source_url=c82u_dataset_url,
            ),
            build_graph_node(
                bissna_company_node_id,
                "BISSNA COMUNICACIONES S.A.S.",
                "company",
                document_id="901724535",
                properties={
                    "name": "BISSNA COMUNICACIONES S.A.S.",
                    "razon_social": "BISSNA COMUNICACIONES S.A.S.",
                    "nit": "901724535",
                    "fecha_matricula": "20230619",
                    "ultimo_ano_renovado": "2025",
                    "estado_matricula": "ACTIVA",
                    "evidence_tier": "registro_oficial",
                },
                source_name="registro_mercantil_abierto_c82u_588k",
                source_url=c82u_dataset_url,
            ),
            build_graph_node(
                lc_gutierrez_company_node_id,
                "LC GUTIERREZ ASESORIAS S.A.S",
                "company",
                document_id="901904991",
                properties={
                    "name": "LC GUTIERREZ ASESORIAS S.A.S",
                    "razon_social": "LC GUTIERREZ ASESORIAS S.A.S",
                    "nit": "901904991",
                    "fecha_matricula": "20250114",
                    "ultimo_ano_renovado": "2025",
                    "estado_matricula": "ACTIVA",
                    "evidence_tier": "registro_oficial",
                },
                source_name="registro_mercantil_abierto_c82u_588k",
                source_url=c82u_dataset_url,
            ),
            build_graph_node(
                bluhartmann_brand_node_id,
                "BLUHARTMANN",
                "brand",
                properties={
                    "name": "BLUHARTMANN",
                    "website": bluhartmann_url,
                    "role": "Marca / portafolio público",
                    "evidence_tier": "contexto_publico",
                },
                source_name="behance_bluhartmann_profile",
                source_url=bluhartmann_behance_profile_url,
            ),
            build_graph_node(
                hazte_profesional_node_id,
                "HazteProfesional.com",
                "website",
                properties={
                    "name": "HazteProfesional.com",
                    "website": hazte_profesional_url,
                    "role": "Sitio público de captación y oferta académica",
                    "description": "Somos la USanJosé",
                    "public_ip": "191.96.244.28",
                    "evidence_tier": "contexto_publico",
                },
                source_name="hazte_profesional_public_site",
                source_url=hazte_profesional_url,
            ),
            build_graph_node(
                staging_hazte_node_id,
                "staging.hazteprofesional.com",
                "website",
                properties={
                    "name": "staging.hazteprofesional.com",
                    "website": staging_hazte_url,
                    "role": "Sistema de horarios de San Jose",
                    "public_ip": "191.96.244.28",
                    "page_title": "Sistema de horarios de San Jose",
                    "evidence_tier": "contexto_publico",
                },
                source_name="staging_hazte_profesional_wp_api",
                source_url=staging_hazte_wp_api_url,
            ),
            build_graph_node(
                i_hazte_node_id,
                "i.hazteprofesional.com",
                "website",
                properties={
                    "name": "i.hazteprofesional.com",
                    "website": i_hazte_url,
                    "role": "Proceso de inscripcion",
                    "public_ip": "190.71.217.85",
                    "page_title": "Proceso de Inscripción",
                    "evidence_tier": "contexto_publico",
                },
                source_name="i_hazte_profesional_public_app",
                source_url=i_hazte_url,
            ),
            build_graph_node(
                alianzas_hazte_node_id,
                "alianzas.hazteprofesional.com",
                "website",
                properties={
                    "name": "alianzas.hazteprofesional.com",
                    "website": alianzas_hazte_url,
                    "role": "Formulario de alianzas",
                    "public_ip": "190.71.217.85",
                    "evidence_tier": "contexto_publico",
                },
                source_name="alianzas_hazte_profesional_public_app",
                source_url=alianzas_hazte_url,
            ),
            build_graph_node(
                convenios_bluhartmann_node_id,
                "convenios.bluhartmann.com",
                "website",
                properties={
                    "name": "convenios.bluhartmann.com",
                    "website": convenios_bluhartmann_url,
                    "role": "Portal de convenios",
                    "public_ip": "191.96.244.28",
                    "page_title": "BluHartmann",
                    "evidence_tier": "contexto_publico",
                },
                source_name="convenios_bluhartmann_wp_api",
                source_url=convenios_bluhartmann_wp_api_url,
            ),
            build_graph_node(
                pagos_bluhartmann_node_id,
                "pagos.bluhartmann.com",
                "website",
                properties={
                    "name": "pagos.bluhartmann.com",
                    "website": pagos_bluhartmann_url,
                    "role": "Pagos Bluhartmann",
                    "public_ip": "190.71.217.85",
                    "page_title": "Pagos Bluhartmann",
                    "evidence_tier": "contexto_publico",
                },
                source_name="pagos_bluhartmann_public_app",
                source_url=pagos_bluhartmann_url,
            ),
        ]
    )

    for name, nit, fecha_matricula in andres_registry_companies[1:]:
        documented_nodes.append(
            build_graph_node(
                f"documented:company:{nit}",
                name,
                "company",
                document_id=nit,
                properties={
                    "name": name,
                    "nit": nit,
                    "fecha_matricula": fecha_matricula,
                    "ultimo_ano_renovado": "2025",
                    "evidence_tier": "registro_oficial",
                },
                source_name="registro_mercantil_abierto_c82u_588k",
                source_url=c82u_dataset_url,
            )
        )
    for name, nit, fecha_matricula in mauricio_registry_companies:
        documented_nodes.append(
            build_graph_node(
                f"documented:company:{nit}",
                name,
                "company",
                document_id=nit,
                properties={
                    "name": name,
                    "nit": nit,
                    "fecha_matricula": fecha_matricula,
                    "ultimo_ano_renovado": "2025",
                    "evidence_tier": "registro_oficial",
                },
                source_name="registro_mercantil_abierto_c82u_588k",
                source_url=c82u_dataset_url,
            )
        )

    documented_edges.extend(
        [
            build_graph_edge(
                stefanny_node_id,
                san_jose_center,
                "ROLE_DOCUMENTED",
                confidence=0.98,
                properties={
                    "role": "Vicerrectora Académica",
                    "match_reason": "registro oficial · sitio institucional y política de seguridad FESSJ 2024",
                },
            ),
            build_graph_edge(
                luis_node_id,
                san_jose_center,
                "ROLE_DOCUMENTED",
                confidence=0.98,
                properties={
                    "role": "Secretario General / Director de Planeación",
                    "match_reason": "registro oficial · política de seguridad FESSJ 2024",
                },
            ),
            build_graph_edge(
                mauricio_node_id,
                icaft_center_id,
                "ROLE_DOCUMENTED",
                confidence=0.98,
                properties={
                    "role": "Representante legal (certificado ICAFT 2024)",
                    "match_reason": "registro oficial · certificado ICAFT / MEN 2024",
                },
            ),
            build_graph_edge(
                mauricio_node_id,
                icaft_center_id,
                "ROLE_DOCUMENTED",
                confidence=0.99,
                properties={
                    "role": "Representante legal suplente (RUT 2026)",
                    "match_reason": "registro oficial · RUT público ICAFT 2026",
                },
            ),
            build_graph_edge(
                andres_node_id,
                icaft_center_id,
                "ROLE_DOCUMENTED",
                confidence=0.99,
                properties={
                    "role": "Representante legal principal (RUT 2026)",
                    "match_reason": "registro oficial · RUT público ICAFT 2026",
                },
            ),
            build_graph_edge(
                sonia_node_id,
                icaft_center_id,
                "ROLE_DOCUMENTED",
                confidence=0.99,
                properties={
                    "role": "Socia / miembro en RUT 2026",
                    "match_reason": "registro oficial · RUT público ICAFT 2026",
                    "fecha_ingreso": "20240324",
                },
            ),
            build_graph_edge(
                clara_node_id,
                icaft_center_id,
                "ROLE_DOCUMENTED",
                confidence=0.99,
                properties={
                    "role": "Socia / miembro en RUT 2026",
                    "match_reason": "registro oficial · RUT público ICAFT 2026",
                    "fecha_ingreso": "20240814",
                },
            ),
            build_graph_edge(
                mauricio_florez_nino_node_id,
                icaft_center_id,
                "ROLE_DOCUMENTED",
                confidence=0.99,
                properties={
                    "role": "Socio / miembro en RUT 2026",
                    "match_reason": "registro oficial · RUT público ICAFT 2026",
                    "fecha_ingreso": "20240814",
                },
            ),
            build_graph_edge(
                david_reyes_devia_node_id,
                icaft_center_id,
                "ROLE_DOCUMENTED",
                confidence=0.99,
                properties={
                    "role": "Socio / miembro en RUT 2025-09-26",
                    "match_reason": "registro oficial · RUT público ICAFT 2025-09-26",
                    "fecha_ingreso": "20240814",
                },
            ),
            build_graph_edge(
                andres_node_id,
                business_labs_node_id,
                "REPRESENTA_LEGALMENTE",
                confidence=0.99,
                properties={
                    "match_reason": "registro mercantil abierto c82u-588k",
                    "role": "Representante legal",
                },
            ),
            build_graph_edge(
                eloira_node_id,
                lc_gutierrez_company_node_id,
                "REPRESENTA_LEGALMENTE",
                confidence=0.99,
                properties={
                    "match_reason": "registro mercantil abierto c82u-588k",
                    "role": "Representante legal",
                },
            ),
            build_graph_edge(
                carlos_adolfo_node_id,
                bissna_company_node_id,
                "REPRESENTA_LEGALMENTE",
                confidence=0.99,
                properties={
                    "match_reason": "registro mercantil abierto c82u-588k",
                    "role": "Representante legal",
                },
            ),
            build_graph_edge(
                bluhartmann_brand_node_id,
                business_labs_node_id,
                "PUBLIC_PROFILE_TRACE",
                confidence=0.84,
                properties={
                    "role": "Perfil público BLUHARTMANN se identifica con Business Labs United / bluhartmann.com",
                    "match_reason": "contexto público · Behance BLUHARTMANN",
                },
            ),
            build_graph_edge(
                andres_node_id,
                bluhartmann_brand_node_id,
                "PUBLIC_PORTFOLIO_TRACE",
                confidence=0.82,
                properties={
                    "role": "Owner público en proyectos GRADOS ICAFT / GRADOS USANJOSÉ",
                    "match_reason": "contexto público · Behance BLUHARTMANN",
                },
            ),
            build_graph_edge(
                bluhartmann_brand_node_id,
                icaft_center_id,
                "PUBLIC_PORTFOLIO_TRACE",
                confidence=0.82,
                properties={
                    "role": "Proyecto público GRADOS ICAFT | BLUHARTMANN",
                    "match_reason": "contexto público · Behance 2025-02-25",
                },
            ),
            build_graph_edge(
                bluhartmann_brand_node_id,
                san_jose_center,
                "PUBLIC_PORTFOLIO_TRACE",
                confidence=0.82,
                properties={
                    "role": "Proyecto público GRADOS USANJOSÉ | NACIDOS PARA BRILLAR | BLUHARTMANN",
                    "match_reason": "contexto público · Behance 2025-03-10",
                },
            ),
            build_graph_edge(
                hazte_profesional_node_id,
                san_jose_center,
                "PUBLIC_MARKETING_TRACE",
                confidence=0.8,
                properties={
                    "role": "Sitio público de captación con logos y PDFs de programas USJ",
                    "match_reason": "contexto público · hazteprofesional.com",
                },
            ),
            build_graph_edge(
                hazte_profesional_node_id,
                bluhartmann_brand_node_id,
                "PUBLIC_INFRASTRUCTURE_TRACE",
                confidence=0.76,
                properties={
                    "role": "Comparte IP pública 191.96.244.28 y nameservers ns1/ns2.dns-parking.com",
                    "match_reason": "contexto público · DNS / hosting",
                },
            ),
            build_graph_edge(
                staging_hazte_node_id,
                hazte_profesional_node_id,
                "PUBLIC_SUBDOMAIN_TRACE",
                confidence=0.86,
                properties={
                    "role": "Subdominio publico de HazteProfesional",
                    "match_reason": "contexto publico · DNS / WordPress wp-json",
                },
            ),
            build_graph_edge(
                i_hazte_node_id,
                hazte_profesional_node_id,
                "PUBLIC_SUBDOMAIN_TRACE",
                confidence=0.86,
                properties={
                    "role": "Subdominio publico de HazteProfesional",
                    "match_reason": "contexto publico · DNS / HTML publico",
                },
            ),
            build_graph_edge(
                alianzas_hazte_node_id,
                hazte_profesional_node_id,
                "PUBLIC_SUBDOMAIN_TRACE",
                confidence=0.84,
                properties={
                    "role": "Subdominio publico de HazteProfesional",
                    "match_reason": "contexto publico · DNS / HTML publico",
                },
            ),
            build_graph_edge(
                convenios_bluhartmann_node_id,
                bluhartmann_brand_node_id,
                "PUBLIC_SUBDOMAIN_TRACE",
                confidence=0.86,
                properties={
                    "role": "Subdominio publico de BluHartmann",
                    "match_reason": "contexto publico · DNS / WordPress wp-json",
                },
            ),
            build_graph_edge(
                pagos_bluhartmann_node_id,
                bluhartmann_brand_node_id,
                "PUBLIC_SUBDOMAIN_TRACE",
                confidence=0.86,
                properties={
                    "role": "Subdominio publico de BluHartmann",
                    "match_reason": "contexto publico · DNS / HTML publico",
                },
            ),
            build_graph_edge(
                staging_hazte_node_id,
                san_jose_center,
                "PUBLIC_APPLICATION_TRACE",
                confidence=0.87,
                properties={
                    "role": "Sistema de horarios de San Jose con busqueda por numero de cedula",
                    "match_reason": "contexto publico · wp-json / HTML publico",
                },
            ),
            build_graph_edge(
                i_hazte_node_id,
                san_jose_center,
                "PUBLIC_APPLICATION_TRACE",
                confidence=0.87,
                properties={
                    "role": "Proceso de inscripcion con logos USANJOSE, paz y salvo e invitados a grados",
                    "match_reason": "contexto publico · HTML publico",
                },
            ),
            build_graph_edge(
                convenios_bluhartmann_node_id,
                bluhartmann_brand_node_id,
                "PUBLIC_APPLICATION_TRACE",
                confidence=0.84,
                properties={
                    "role": "Portal publico de convenios / login",
                    "match_reason": "contexto publico · wp-json / HTML publico",
                },
            ),
            build_graph_edge(
                pagos_bluhartmann_node_id,
                bluhartmann_brand_node_id,
                "PUBLIC_APPLICATION_TRACE",
                confidence=0.84,
                properties={
                    "role": "Portal publico de pagos con referencia de pago",
                    "match_reason": "contexto publico · HTML publico",
                },
            ),
            build_graph_edge(
                staging_hazte_node_id,
                convenios_bluhartmann_node_id,
                "PUBLIC_INFRASTRUCTURE_TRACE",
                confidence=0.79,
                properties={
                    "role": "Comparte IP publica 191.96.244.28",
                    "match_reason": "contexto publico · DNS / infraestructura",
                },
            ),
            build_graph_edge(
                i_hazte_node_id,
                pagos_bluhartmann_node_id,
                "PUBLIC_INFRASTRUCTURE_TRACE",
                confidence=0.79,
                properties={
                    "role": "Comparte IP publica 190.71.217.85",
                    "match_reason": "contexto publico · DNS / infraestructura",
                },
            ),
            build_graph_edge(
                i_hazte_node_id,
                pagos_bluhartmann_node_id,
                "PUBLIC_APPLICATION_TRACE",
                confidence=0.82,
                properties={
                    "role": "Ambos portales publicos usan fetch('service/') para flujo de pagos / validacion",
                    "match_reason": "contexto publico · HTML / JavaScript visible",
                },
            ),
            build_graph_edge(
                i_hazte_node_id,
                alianzas_hazte_node_id,
                "PUBLIC_APPLICATION_TRACE",
                confidence=0.8,
                properties={
                    "role": "Ambos portales Hazte envian JSON a un endpoint relativo service/",
                    "match_reason": "contexto publico · HTML / JavaScript visible",
                },
            ),
        ]
    )

    for name, nit, _fecha_matricula in andres_registry_companies[1:]:
        documented_edges.append(
            build_graph_edge(
                andres_node_id,
                f"documented:company:{nit}",
                "REPRESENTA_LEGALMENTE",
                confidence=0.99,
                properties={
                    "match_reason": "registro mercantil abierto c82u-588k",
                    "role": "Representante legal",
                },
            )
        )
    for name, nit, _fecha_matricula in mauricio_registry_companies:
        documented_edges.append(
            build_graph_edge(
                mauricio_node_id,
                f"documented:company:{nit}",
                "REPRESENTA_LEGALMENTE",
                confidence=0.99,
                properties={
                    "match_reason": "registro mercantil abierto c82u-588k",
                    "role": "Representante legal",
                },
            )
        )

    graph = merge_graph_payload(
        graph,
        extra_nodes=documented_nodes,
        extra_edges=[edge for edge in documented_edges if edge["source"] and edge["target"]],
    )

    findings.append(
        "Documentos institucionales de San José agregan dos actores que no estaban en el grafo base: "
        "Leidy Stefanny Camacho Galindo como Vicerrectora Académica y Luis Carlos Gutiérrez Martínez como "
        "Secretario General / Director de Planeación."
    )
    if icaft_center_node:
        findings.append(
            "ICAFT aporta una segunda capa documental verificable: el certificado institucional disponible "
            "en su sitio identifica a Mauricio Guevara Marín como representante legal."
        )
    findings.append(
        "El RUT público de ICAFT generado el 2026-03-01 amplía esa capa oficial: allí aparecen Andrés David "
        "Méndez Flórez como representante legal principal y Mauricio Guevara Marín como suplente."
    )
    findings.append(
        "La cronología pública ya se puede leer con dos RUTs: el del 2025-09-26 mostraba a Mauricio Guevara "
        "Marín como representante legal principal y a Andrés David Méndez Flórez como suplente; el del "
        "2026-03-01 invierte esos cargos."
    )
    findings.append(
        "Ese mismo RUT mete varios nombres del reportaje dentro de ICAFT con documento y fecha: Sonia Isabel "
        "Méndez Flórez, Clara Inés Galindo Díaz y Mauricio Flórez Niño figuran como socios o miembros."
    )
    findings.append(
        "La misma comparación entre RUTs también deja un cambio de composición: David Fernando Reyes Devia "
        "aparece en el RUT del 2025-09-26 y ya no aparece en el publicado el 2026-03-01, donde entra Clara "
        "Inés Galindo Díaz."
    )
    findings.append(
        "El registro mercantil abierto c82u-588k identifica a BUSINESS LABS UNITED S. A. S. "
        "(NIT 901498512-5) como sociedad activa, matriculada el 2021-07-02 y con Andrés David Méndez Flórez "
        "como representante legal."
    )
    findings.append(
        "El sitio público bluhartmann.com usa la marca BluHartmann / BLUHARTMANN OFFICIAL BRAND, pero c82u-588k "
        "no devuelve por ahora una persona jurídica con ese nombre exacto. El hit mercantil verificable bajo "
        "Andrés sigue siendo BUSINESS LABS UNITED S. A. S."
    )
    findings.append(
        "Ese mismo registro mercantil muestra a Andrés David Méndez Flórez como representante legal de 4 "
        "sociedades activas: BUSINESS LABS UNITED S. A. S., INNOVATION QUALITY SUPPORT S.A.S., SOLUCIONES EN "
        "TECNOLOGIA E INGENIERIA SAS y BENSON&CLAIRE S.A.S."
    )
    findings.append(
        "El mismo registro mercantil muestra a Mauricio Guevara Marín como representante legal de 3 "
        "sociedades activas: JOY 1 SAS, SOCIEDAD DE INVERSIONES INMOBILIARIAS COGOLLO S.A.S. y "
        "SOCIEDAD DE INVERSIONES INMOBILIARIAS MARTINEZ & CIA SAS."
    )
    findings.append(
        "Ese mismo registro mercantil también confirma a ELOIRA MARGOTH COGOLLO CARO como representante "
        "legal de LC GUTIERREZ ASESORIAS S.A.S. (NIT 901904991), sociedad activa matriculada el 2025-01-14."
    )
    findings.append(
        "La segunda capa societaria gana peso con apellidos repetidos en registros abiertos: Mauricio "
        "Guevara representa sociedades con razón social COGOLLO y MARTINEZ, mientras Eloira aparece al frente "
        "de LC GUTIERREZ ASESORIAS S.A.S. Eso no prueba parentesco, pero sí deja una pista nominal verificable "
        "para seguir el hilo del reportaje."
    )
    findings.append(
        "Las capturas aportadas el 22 de marzo también abren una línea verificable adicional: c82u-588k "
        "confirma a BISSNA COMUNICACIONES S.A.S. (NIT 901724535) como sociedad activa, matriculada el "
        "2023-06-19 y con CARLOS ADOLFO MENDEZ FLOREZ como representante legal."
    )
    findings.append(
        "La capa web pública también deja un rastro concreto de operación comercial: hazteprofesional.com "
        "sirve logos SANJOSEBLANCO / LOGO-SAN-JOSE y PDFs de programas USJ, mientras el portafolio público "
        "de BLUHARTMANN exhibe piezas GRADOS ICAFT y GRADOS USANJOSÉ."
    )
    findings.append(
        "HazteProfesional agrega una segunda huella pública útil: su WordPress abierto se presenta como "
        "\"Somos la USanJosé\" y comparte IP pública 191.96.244.28 y nameservers con bluhartmann.com, "
        "mientras San José e ICAFT viven en infraestructuras distintas."
    )
    findings.append(
        "El rastreo puntual sobre la IP 191.96.244.28 deja una capa mas fuerte que el simple hosting: "
        "staging.hazteprofesional.com publica un 'Sistema de horarios de San Jose' con buscador por numero "
        "de cedula sobre 'horarios-estudian', y convenios.bluhartmann.com publica un WordPress con nombre "
        "'BluHartmann'."
    )
    findings.append(
        "La capa de aplicaciones espejo se repite en otra IP publica, 190.71.217.85: "
        "i.hazteprofesional.com expone un 'Proceso de Inscripcion' con logos USANJOSE y menciones a paz "
        "y salvo / invitados a grados, mientras pagos.bluhartmann.com expone 'Pagos Bluhartmann' y un "
        "campo 'Referencia de Pago'."
    )
    findings.append(
        "HazteProfesional tambien expone alianzas.hazteprofesional.com como formulario publico de "
        "captacion por alianzas, y el HTML del sitio principal carga un child theme llamado "
        "'Divi-blu-child'. Eso no prueba control comun, pero suma una pista tecnica adicional junto con "
        "los subdominios y certificados publicos."
    )
    findings.append(
        "La capa operativa visible tambien se parece entre portales: i.hazteprofesional.com, "
        "alianzas.hazteprofesional.com y pagos.bluhartmann.com hacen POST JSON a un endpoint relativo "
        "'service/'. En el HTML publico aparecen acciones como getCountries, replyWithAI, handlePayment, "
        "checkPayment, getDebt y getPaymentURL."
    )
    findings.append(
        "convenios.bluhartmann.com muestra una segunda familia tecnica: un formulario JetFormBuilder con "
        "submit AJAX (?jet_form_builder_submit=submit&method=ajax) y campos de nombre, correo, Whatsapp, "
        "nivel academico, carrera a estudiar y alianza."
    )
    findings.append(
        "Ya existe un rastro público verificable BLUHARTMANN -> ICAFT / USANJOSÉ y BLUHARTMANN -> Business "
        "Labs United, pero todavía no aparece un puente registral o contractual abierto que una ese cluster "
        "mercantil con la contratación de San José."
    )

    evidence = [
        build_evidence_item(
            "directivos institucionales",
            str(int(metrics.get("education_director_count") or 0)),
            "registro oficial MEN",
        ),
        build_evidence_item("alias contractual detectado", alias_document or "sí", "cruce directo MEN ↔ SECOP"),
        build_evidence_item(
            "convenios interadministrativos enlazados",
            str(int(metrics.get("education_procurement_link_count") or 0)),
            "SECOP / Apía, Risaralda",
        ),
        build_evidence_item(
            "valor contractual enlazado",
            compact_money(metrics.get("education_procurement_total") or 0),
            "convenios visibles en SECOP",
        ),
        build_evidence_item(
            "funcionarios documentados en San José",
            "2",
            "Stefanny Camacho y Luis Carlos Gutiérrez",
        ),
        build_evidence_item(
            "sociedades activas bajo Andrés Méndez",
            "4",
            "registro mercantil abierto c82u-588k",
        ),
        build_evidence_item(
            "sociedades activas bajo Mauricio Guevara",
            "3",
            "registro mercantil abierto c82u-588k",
        ),
        build_evidence_item(
            "sociedades activas bajo Eloira Cogollo",
            "1",
            "registro mercantil abierto c82u-588k",
        ),
        build_evidence_item(
            "sociedades activas bajo Carlos Adolfo Mendez Florez",
            "1",
            "BISSNA COMUNICACIONES S.A.S. en c82u-588k",
        ),
        build_evidence_item(
            "actores documentados en RUT ICAFT 2026",
            "5",
            "Andrés, Mauricio, Sonia, Clara y Mauricio Flórez Niño",
        ),
        build_evidence_item(
            "subdominios publicos verificados",
            "5",
            "staging / i / alianzas Hazte; convenios / pagos BluHartmann",
        ),
        build_evidence_item(
            "pares de aplicaciones con IP compartida",
            "2",
            "191.96.244.28 y 190.71.217.85",
        ),
        build_evidence_item(
            "portales publicos con backend service/",
            "3",
            "inscripcion, alianzas y pagos",
        ),
        build_evidence_item(
            "formulario AJAX visible en convenios",
            "1",
            "JetFormBuilder submit=ajax",
        ),
    ]
    if icaft_controllers:
        evidence.append(
            build_evidence_item(
                "ICAFT como entidad relacionada",
                str(len(dedupe_strings(icaft_controllers))),
                "controladores cargados en MEN",
            )
        )

    reported_claims = [
        "Marca / operador reportado · El Observatorio (26 de septiembre de 2025) describe a BluHartmann como el frente privado de mercadeo, graduaciones y captacion comercial que empuja el crecimiento de San Jose e ICAFT. La web publica y el portafolio de BLUHARTMANN ya dejan un rastro verificable de esas piezas, aunque no una matricula mercantil directa bajo ese nombre.",
        "Pareja reportada · El Observatorio (26 de septiembre de 2025) describe a Stefanny Camacho como compañera sentimental de Andres David Mendez Florez en la operacion de grados y promocion de San Jose. Inferencia del dossier: esto coincide con la vicerrectora Leidy Stefanny Camacho Galindo documentada en el sitio institucional.",
        "Padres / suegros reportados · La denuncia citada por El Observatorio (26 de febrero de 2026) dice que en un RUT de ICAFT visto en mayo de 2025 aparecian Helminson Camacho Buiche y Clara Ines Galindo Diaz como familiares de Stefanny Camacho y socios del frente ICAFT. El RUT publico vigente ya confirma a Clara Ines Galindo Diaz dentro de ICAFT; Helminson sigue sin puente documental equivalente.",
        "Conyuge reportada · La misma denuncia (26 de febrero de 2026) presenta a Eloira Margoth Cogollo Caro como esposa de Luis Carlos Gutierrez y como parte del segundo anillo societario del caso.",
        "Suplencia reportada · El Observatorio (26 de febrero de 2026) afirma que Andres David Mendez Florez figuro como representante legal suplente de ICAFT. El RUT publico publicado por ICAFT el 2026-03-01 hoy muestra el reparto inverso: Andres como principal y Mauricio Guevara Marin como suplente, lo que obliga a reconstruir la cronologia exacta del cambio.",
        "Hermanos reportados · La pieza periodistica del 22 de marzo de 2026 nombra a Carlos Mendez Florez, Oscar Mendez Florez y Sonia Isabel Mendez Florez dentro del anillo familiar que orbita el caso. Sonia ya aparece como miembro en el RUT publico de ICAFT; Carlos conserva un posible match registral por la via de Carlos Adolfo Mendez Florez y BISSNA COMUNICACIONES; Oscar sigue sin match abierto limpio.",
    ]
    reported_sources = [
        observatorio_machinery_url,
        observatorio_family_url,
    ]
    verified_open_data = [
        "MEN y el grafo cargado confirman 2 controladores institucionales en San Jose y un alias SECOP que conecta con 2 convenios interadministrativos en Apia por COP 872.6M.",
        "El sitio institucional de San Jose confirma a Leidy Stefanny Camacho Galindo y Luis Carlos Gutierrez Martinez en cargos directivos dentro de la institucion.",
        "El certificado institucional de ICAFT confirma a Mauricio Guevara Marin como representante legal, y el RUT publico de ICAFT generado el 2026-03-01 agrega a Andres David Mendez Florez como representante legal principal y a Mauricio Guevara Marin como suplente.",
        "El RUT publico de ICAFT del 2025-09-26 mostraba el reparto inverso: Mauricio Guevara Marin como representante legal principal y Andres David Mendez Florez como suplente. La inversion queda documentada con dos RUTs publicos fechados.",
        "Ese mismo RUT publico de ICAFT lista como socios o miembros a Sonia Isabel Mendez Florez, Clara Ines Galindo Diaz, Mauricio Florez Niño, Mauricio Guevara Marin y Andres David Mendez Florez, con fechas de ingreso visibles.",
        "El RUT publico de ICAFT del 2025-09-26 lista a DAVID FERNANDO REYES DEVIA y el del 2026-03-01 ya no lo muestra; en ese segundo documento aparece CLARA INES GALINDO DIAZ.",
        "El sitio publico bluhartmann.com usa la marca BluHartmann / BLUHARTMANN OFFICIAL BRAND, pero c82u-588k no devuelve por ahora una persona juridica con ese nombre exacto.",
        "El dataset abierto c82u-588k confirma que BUSINESS LABS UNITED S. A. S. es una sociedad activa con NIT 901498512-5 y que Andres David Mendez Florez figura como representante legal.",
        "El mismo c82u-588k confirma a Andres David Mendez Florez como representante legal de 4 sociedades activas y a Mauricio Guevara Marin como representante legal de 3 sociedades activas.",
        "El mismo c82u-588k confirma a ELOIRA MARGOTH COGOLLO CARO como representante legal de LC GUTIERREZ ASESORIAS S.A.S. (NIT 901904991), matriculada el 2025-01-14.",
        "El registro abierto tambien deja una pista nominal verificable: Mauricio Guevara Marin representa sociedades con razon social COGOLLO y MARTINEZ, creadas en marzo de 2025.",
        "El mismo c82u-588k confirma a BISSNA COMUNICACIONES S.A.S. (NIT 901724535) como sociedad activa desde el 2023-06-19 y a CARLOS ADOLFO MENDEZ FLOREZ como su representante legal.",
        "El nombre de Carlos conserva una pista registral parcial por la via de CARLOS ADOLFO MENDEZ FLOREZ y BISSNA COMUNICACIONES S.A.S.; el puente familiar con Andres no queda probado solo con ese match nominal.",
        "hazteprofesional.com exhibe logos SANJOSEBLANCO / LOGO-SAN-JOSE y aloja PDFs de programas USJ, lo que deja un rastro publico verificable de captacion alrededor de San Jose.",
        "La API publica de WordPress de hazteprofesional.com describe el sitio como \"Somos la USanJosé\" y el dominio comparte IP publica 191.96.244.28 y los mismos nameservers que bluhartmann.com.",
        "hazteprofesional.com carga publicamente el stylesheet /wp-content/themes/Divi-blu-child/style.css; ese artefacto tecnico no prueba propiedad ni control, pero si agrega una pista publica de implementacion.",
        "Los certificados publicos de hazteprofesional.com muestran al menos los subdominios staging.hazteprofesional.com, i.hazteprofesional.com y alianzas.hazteprofesional.com; los de bluhartmann.com muestran convenios.bluhartmann.com y pagos.bluhartmann.com.",
        "La API publica de staging.hazteprofesional.com lo nombra \"Sistema de horarios de San Jose\" y el HTML expone busqueda por numero de cedula sobre el origen horarios-estudian.",
        "El HTML publico de i.hazteprofesional.com expone un flujo \"Proceso de Inscripcion\" con logos USANJOSE y menciones a paz y salvo e invitados a la ceremonia de grados.",
        "La API publica de convenios.bluhartmann.com lo nombra \"BluHartmann\" y el HTML visible deja un rastro de portal de convenios / login.",
        "El HTML publico de pagos.bluhartmann.com expone un flujo \"Pagos Bluhartmann\" con campo \"Referencia de Pago\".",
        "El HTML publico de alianzas.hazteprofesional.com expone un formulario de registro con selector \"Alianza\" y llamado a captacion de interesados.",
        "staging.hazteprofesional.com y convenios.bluhartmann.com comparten IP publica 191.96.244.28; i.hazteprofesional.com y pagos.bluhartmann.com comparten IP publica 190.71.217.85.",
        "El HTML publico de i.hazteprofesional.com muestra un fetch('service/') con acciones replyWithAI, getCountries, handlePayment y checkPayment, ademas de personKey y flujo OTP.",
        "El HTML publico de pagos.bluhartmann.com muestra un fetch('service/') con acciones getDebt, checkPayment y getPaymentURL, junto con transactionTicketId y reCAPTCHA.",
        "El HTML publico de alianzas.hazteprofesional.com muestra un fetch('service/') con acciones setFormSimple y getAllOptions para serializar campos enc_capture.",
        "El HTML publico de convenios.bluhartmann.com expone un formulario JetFormBuilder con submit AJAX y campos de nombre, correo, Whatsapp, nivel academico, carrera y alianza.",
        "El perfil publico Behance de BLUHARTMANN se presenta como Business Labs United / bluhartmann.com y publica proyectos GRADOS ICAFT y GRADOS USANJOSÉ, lo que agrega un rastro publico verificable entre la marca y ambas instituciones.",
        "Las busquedas exactas en el grafo SECOP cargado y en los registros abiertos de proveedores no encontraron, por ahora, ni a BUSINESS LABS UNITED ni a BISSNA COMUNICACIONES ni a las otras sociedades del cluster como proveedores registrados o contratistas visibles.",
    ]
    open_questions = [
        "Falta un puente registral o contractual abierto BLUHARTMANN -> BUSINESS LABS UNITED -> San Jose o ICAFT. Hoy ya existe rastro publico de portafolio, pero no una matricula compartida ni un contrato visible que cierre ese circuito.",
        "Las fuentes periodisticas usan dos variantes para Luis Carlos Gutierrez: Martinez y Ramirez. Falta un documento abierto con cedula que cierre esa identidad sin ambiguedad.",
        "Falta una fuente abierta y verificable para probar la relacion personal reportada entre Andres David Mendez Florez y Leidy / Stefanny Camacho Galindo.",
        "Clara Ines Galindo Diaz ya aparece en el RUT publico de ICAFT, pero falta un documento abierto que la conecte formalmente con Stefanny Camacho y falta un soporte equivalente para Helminson Camacho Buiche.",
        "Falta cerrar si el Carlos Mendez Florez del video y el CARLOS ADOLFO MENDEZ FLOREZ del registro mercantil son la misma persona, y sigue faltando un match abierto equivalente para Oscar Mendez Florez.",
        "La inversion de representacion entre los RUT del 2025-09-26 y el 2026-03-01 ya esta documentada. Falta el acto societario o administrativo abierto que explique el cambio y la salida de David Fernando Reyes Devia.",
        "HazteProfesional y BLUHARTMANN comparten huella de hosting, pero esa coincidencia no prueba por si sola propiedad ni control comun.",
        "Los subdominios y portales publicos dejan un rastro tecnico mas fuerte que el simple hosting, pero siguen sin probar por si solos propiedad comun ni integracion contractual entre HazteProfesional, BluHartmann, San Jose e ICAFT.",
        "Hace falta identificar si los tres portales que usan el endpoint relativo service/ comparten backend, proveedor o base operativa comun. Hoy la similitud queda documentada a nivel de HTML y flujo publico, no a nivel de propiedad.",
        "Hace falta cerrar si convenios.bluhartmann.com recoge leads para San Jose / ICAFT o para otros frentes comerciales. El formulario visible no menciona una institucion concreta en el mismo nivel de claridad que Hazte.",
        "El certificado publico de bluhartmann.com tambien muestra alianzas.bluhartmann.com, pero en esta revision no devolvio una pagina publica estable ni quedo conectado a las IPs centrales 191.96.244.28 / 190.71.217.85.",
        "Falta una fuente abierta que permita contrastar las afirmaciones sobre ingresos, capital y contratacion cruzada mencionadas por los periodistas.",
    ]

    return {
        "slug": "san-jose-icaft-network",
        "title": "Fundación San José / ICAFT: control educativo y segundo anillo societario verificable",
        "category": "captura_educativa",
        "status": "public_case",
        "entity_id": entity_id,
        "entity_type": "company",
        "subject_name": case.get("entity_name"),
        "subject_ref": case.get("entity_ref"),
        "summary": "El dossier ya no se apoya solo en el relato periodístico: une control institucional MEN, alias SECOP y convenios de Apía con el RUT público 2026 de ICAFT, el segundo anillo mercantil verificable de Business Labs / BISSNA / Eloira y una nueva capa de aplicaciones públicas Hazte / BluHartmann, sin convertir en hecho lo que aún no tiene soporte abierto.",
        "why_it_matters": "Sirve para abrir una revisión documental con actores nominales, cambios de representación en ICAFT, miembros listados en el RUT, alias contractuales, convenios concretos, un clúster mercantil ampliado y ahora también un rastro técnico de aplicaciones públicas emparejadas, manteniendo separadas las hipótesis periodísticas y los puentes familiares o contractuales que todavía no cierran con registro abierto.",
        "findings": findings,
        "evidence": evidence[:14],
        "reported_claims": reported_claims,
        "reported_sources": reported_sources,
        "verified_open_data": verified_open_data,
        "open_questions": open_questions,
        "tags": [format_signal_label(signal) for signal in (case.get("observed_signals") or [])[:4]],
        "public_sources": dedupe_strings(
            [
                men_institutions_dataset_url,
                men_directors_dataset_url,
                c82u_dataset_url,
                bluhartmann_url,
                hazte_profesional_url,
                hazte_profesional_wp_api_url,
                hazte_profesional_page_url,
                staging_hazte_url,
                staging_hazte_wp_api_url,
                staging_hazte_page_url,
                i_hazte_url,
                alianzas_hazte_url,
                convenios_bluhartmann_url,
                convenios_bluhartmann_wp_api_url,
                pagos_bluhartmann_url,
                hazte_certificates_url,
                bluhartmann_certificates_url,
                bluhartmann_behance_profile_url,
                bluhartmann_icaft_project_url,
                bluhartmann_usj_project_url,
                minedu_case_url,
                minedu_resolution_url,
                san_jose_directory_url,
                san_jose_policy_url,
                icaft_docs_url,
                icaft_fundadores_url,
                icaft_certificate_url,
                icaft_rut_sep_2025_url,
                icaft_rut_url,
                observatorio_machinery_url,
                observatorio_family_url,
            ]
        ),
        "graph": graph,
    }


def build_fondecun_investigation(
    stalled_case: dict[str, Any],
    overlap_case: dict[str, Any],
    graph: dict[str, Any] | None,
) -> dict[str, Any]:
    stalled_metrics = stalled_case.get("metrics") or {}
    overlap_metrics = overlap_case.get("metrics") or {}
    graph_metrics = summarize_company_graph(graph)
    findings = [
        f"El sistema reúne {int(stalled_metrics.get('contract_count') or 0)} contratos por {compact_money(stalled_metrics.get('contract_value') or 0)} sobre el mismo actor.",
        f"En esa muestra aparece al menos {int(stalled_metrics.get('execution_gap_contract_count') or 0)} contrato con brecha de ejecución y {compact_money(stalled_metrics.get('execution_gap_invoice_total') or 0)} facturados por delante del avance.",
        f"El mismo actor también dispara cruce con cargo público: {int(overlap_metrics.get('official_officer_count') or 0)} directivo(s) y {int(overlap_metrics.get('official_role_count') or 0)} rol(es) públicos vinculados.",
    ]
    if int(graph_metrics.get("archive_contract_count") or 0) > 0:
        findings.append(
            f"El expediente SECOP II agrega {int(graph_metrics.get('archive_contract_count') or 0)} contrato(s) con {int(graph_metrics.get('archive_document_total') or 0)} referencia(s) documentales públicas."
        )
    evidence = build_metrics_evidence(
        {**stalled_metrics, **overlap_metrics, **graph_metrics},
        [
            "contract_count",
            "contract_value",
            "execution_gap_contract_count",
            "execution_gap_invoice_total",
            "official_officer_count",
            "archive_contract_count",
            "archive_document_total",
            "archive_supervision_contract_count",
            "archive_start_record_contract_count",
        ],
    )
    return {
        "slug": "fondecun-stalled-work",
        "title": "FONDECUN: obra trabada con pagos por delante y traslape oficial",
        "category": "elefante_blanco",
        "status": "public_case",
        "entity_id": stalled_case.get("entity_id"),
        "entity_type": "company",
        "subject_name": stalled_case.get("entity_name"),
        "subject_ref": stalled_case.get("entity_ref"),
        "summary": "FONDECUN sigue siendo una de las validaciones más completas porque el mismo actor concentra brechas de ejecución, convenios interadministrativos y cruce con cargo público.",
        "why_it_matters": "No es una señal aislada: la entidad acumula varias familias de riesgo en la misma red de relaciones y reproduce un caso público ya conocido.",
        "findings": findings,
        "evidence": evidence,
        "tags": [format_signal_label(signal) for signal in (stalled_case.get("observed_signals") or [])[:5]],
        "public_sources": dedupe_strings(
            [
                *combine_public_sources(stalled_case, overlap_case),
                *(
                    [DATASET_URLS["secop_document_archives"]]
                    if int(graph_metrics.get("archive_contract_count") or 0) > 0
                    else []
                ),
            ]
        ),
        "graph": graph,
    }


def build_sanction_window_investigation(
    case: dict[str, Any],
    graph: dict[str, Any] | None,
) -> dict[str, Any]:
    metrics = case.get("metrics") or {}
    graph_metrics = summarize_company_graph(graph)
    findings = [
        f"La empresa mantiene {int(metrics.get('sanction_count') or 0)} antecedente(s) sancionatorios en la batería pública cargada.",
        f"El sistema detecta {int(metrics.get('sanctioned_still_receiving_contract_count') or 0)} contrato dentro de una ventana sancionatoria por {compact_money(metrics.get('sanctioned_still_receiving_total') or 0)}.",
        "Eso convierte la sanción en una verificación útil del sistema, no solo en contexto reputacional.",
    ]
    if int(graph_metrics.get("archive_contract_count") or 0) > 0:
        findings.append(
            f"El expediente SECOP II agrega {int(graph_metrics.get('archive_contract_count') or 0)} contrato(s) con {int(graph_metrics.get('archive_document_total') or 0)} referencia(s) documentales públicas."
        )
    return {
        "slug": "suministros-maybe-sanction-window",
        "title": "Suministros Maybe: contrato dentro de ventana pública de sanción",
        "category": "proveedor_sancionado",
        "status": "public_case",
        "entity_id": case.get("entity_id"),
        "entity_type": "company",
        "subject_name": case.get("entity_name"),
        "subject_ref": case.get("entity_ref"),
        "summary": "El caso sirve para demostrar que el sistema no solo detecta proveedores sancionados: también ubica contratos activos dentro de la ventana de sanción.",
        "why_it_matters": "Es una prueba de trazabilidad temporal entre sanción pública y contratación posterior o concurrente.",
        "findings": findings,
        "evidence": build_metrics_evidence(
            {**metrics, **graph_metrics},
            [
                "sanction_count",
                "contract_count",
                "sanctioned_still_receiving_contract_count",
                "sanctioned_still_receiving_total",
                "archive_contract_count",
                "archive_document_total",
                "archive_payment_contract_count",
            ],
        ),
        "tags": [format_signal_label(signal) for signal in (case.get("observed_signals") or [])[:4]],
        "public_sources": dedupe_strings(
            [
                *combine_public_sources(case),
                *(
                    [DATASET_URLS["secop_document_archives"]]
                    if int(graph_metrics.get("archive_contract_count") or 0) > 0
                    else []
                ),
            ]
        ),
        "graph": graph,
    }


def build_vivian_investigation(
    case: dict[str, Any],
    graph: dict[str, Any] | None,
) -> dict[str, Any]:
    metrics = case.get("metrics") or {}
    findings = [
        f"La misma persona aparece con {int(metrics.get('candidacy_count') or 0)} candidatura(s) y {int(metrics.get('donation_count') or 0)} donación(es) políticas en la carga pública.",
        f"En paralelo, figura con {int(metrics.get('supplier_contract_count') or 0)} contrato(s) como proveedora por {compact_money(metrics.get('supplier_contract_value') or 0)}.",
        f"El sistema además cuenta {int(metrics.get('linked_supplier_company_count') or 0)} empresa(s) proveedoras enlazadas alrededor del mismo documento.",
    ]
    return {
        "slug": "vivian-moreno-political-contractual-overlap",
        "title": "Vivian Moreno: candidatura, donaciones y contratación sobre la misma persona",
        "category": "riesgo_politico_contractual",
        "status": "public_case",
        "entity_id": case.get("entity_id"),
        "entity_type": "person",
        "subject_name": case.get("entity_name"),
        "subject_ref": case.get("entity_ref"),
        "summary": "Este es el ejemplo individual más claro del cruce entre política y contratación pública en los datos publicados.",
        "why_it_matters": "Permite explicar la lógica de 'persona -> política -> provisión al Estado' con un caso verificable y una red de relaciones breve.",
        "findings": findings,
        "evidence": build_metrics_evidence(
            metrics,
            [
                "candidacy_count",
                "donation_count",
                "supplier_contract_count",
                "supplier_contract_value",
                "linked_supplier_company_count",
            ],
        ),
        "tags": [format_signal_label(signal) for signal in (case.get("observed_signals") or [])[:4]],
        "public_sources": combine_public_sources(case),
        "graph": graph,
    }


def build_transmilenio_operator_investigation(
    egobus_case: dict[str, Any],
    coobus_case: dict[str, Any],
    graph: dict[str, Any] | None,
) -> dict[str, Any]:
    egobus_metrics = egobus_case.get("metrics") or {}
    coobus_metrics = coobus_case.get("metrics") or {}
    findings = [
        f"EGOBUS aparece con {int(egobus_metrics.get('sanction_count') or 0)} sanciones públicas registradas.",
        f"COOBUS aparece con {int(coobus_metrics.get('sanction_count') or 0)} sanciones públicas registradas.",
        "El par sirve como control positivo para mostrar que la capa sancionatoria del sistema reproduce operadores ya cuestionados públicamente.",
    ]
    return {
        "slug": "transmilenio-sanctioned-operators",
        "title": "Operadores sancionados de TransMilenio: EGOBUS y COOBUS",
        "category": "proveedor_sancionado",
        "status": "public_case",
        "entity_id": egobus_case.get("entity_id"),
        "entity_type": "company",
        "subject_name": "EGOBUS SAS / COOBUS SAS",
        "subject_ref": f"{egobus_case.get('entity_ref')} · {coobus_case.get('entity_ref')}",
        "summary": "Este caso también sirve como prueba de control: el sistema recupera operadores ya sancionados públicamente.",
        "why_it_matters": "Demuestra que los registros sancionatorios del grafo no son anecdóticos: recuperan actores emblemáticos del caso TransMilenio.",
        "findings": findings,
        "evidence": [
            build_evidence_item("sanciones EGOBUS", str(int(egobus_metrics.get("sanction_count") or 0))),
            build_evidence_item("sanciones COOBUS", str(int(coobus_metrics.get("sanction_count") or 0))),
        ],
        "tags": [format_signal_label(signal) for signal in (egobus_case.get("observed_signals") or [])[:3]],
        "public_sources": combine_public_sources(egobus_case, coobus_case),
        "graph": graph,
    }


def build_official_bulletin_investigation(
    case: dict[str, Any],
    graph: dict[str, Any] | None,
    ungrd_bundle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metrics = case.get("metrics") or {}
    title = str(case.get("title") or "").strip()
    entity_name = str(case.get("entity_name") or "").strip()
    entity_ref = str(case.get("entity_ref") or "").strip()
    findings = [
        f"El sistema reproduce {int(metrics.get('official_case_bulletin_count') or 0)} boletín(es) oficial(es) para este actor dentro del grafo vivo.",
    ]
    if int(metrics.get("person_sanction_count") or 0) > 0:
        findings.append(
            f"Además conserva {int(metrics.get('person_sanction_count') or 0)} antecedente(s) sancionatorio(s) asociados al mismo documento o nodo personal."
        )
    if int(metrics.get("supplier_contract_count") or 0) > 0:
        findings.append(
            f"El mismo sujeto también aparece con {int(metrics.get('supplier_contract_count') or 0)} contrato(s) como proveedor o contratista por {compact_money(metrics.get('supplier_contract_value') or 0)}."
        )
    if int(metrics.get("linked_supplier_company_count") or 0) > 0:
        findings.append(
            f"El cruce llega a {int(metrics.get('linked_supplier_company_count') or 0)} empresa(s) vinculadas alrededor del mismo nombre o documento."
        )
    if int(metrics.get("office_count") or 0) > 0:
        findings.append(
            f"También existe exposición en {int(metrics.get('office_count') or 0)} registro(s) de cargo o salario público."
        )
    ungrd_findings, verified_open_data, open_questions, extra_sources = build_ungrd_document_context(
        case,
        ungrd_bundle,
    )
    findings.extend(ungrd_findings)

    category = str(case.get("category") or "boletin_oficial")
    slug = f"{slugify(entity_name or title)}-{slugify(category)}"
    return {
        "slug": slug,
        "title": title,
        "category": category,
        "status": "public_case",
        "entity_id": case.get("entity_id"),
        "entity_type": case.get("entity_type"),
        "subject_name": entity_name,
        "subject_ref": entity_ref or None,
        "summary": str(case.get("summary") or "").strip(),
        "why_it_matters": "Convierte un boletín oficial en una investigación navegable con el actor, el expediente público y los cruces estructurados que sí existen hoy en el grafo.",
        "findings": findings,
        "evidence": build_metrics_evidence(
            metrics,
            [
                "official_case_bulletin_count",
                "person_sanction_count",
                "supplier_contract_count",
                "supplier_contract_value",
                "linked_supplier_company_count",
                "office_count",
            ],
        ),
        "verified_open_data": verified_open_data,
        "open_questions": open_questions,
        "tags": [format_signal_label(signal) for signal in (case.get("observed_signals") or [])[:5]],
        "public_sources": dedupe_strings([*combine_public_sources(case), *extra_sources]),
        "graph": graph,
    }


def build_generated_sanction_stack_investigation(
    company: dict[str, Any],
    graph: dict[str, Any] | None,
    graph_metrics: dict[str, Any],
) -> dict[str, Any]:
    company_name = str(company.get("name") or "").strip()
    document_id = str(company.get("document_id") or "").strip()
    official_names = list(company.get("official_names") or [])
    findings = [
        f"El actor aparece con {int(company.get('sanction_count') or 0)} antecedente(s) sancionatorios y {int(company.get('sanctioned_still_receiving_contract_count') or 0)} contrato(s) dentro de ventana sancionatoria por {compact_money(company.get('sanctioned_still_receiving_total') or 0)}.",
        f"La capa histórica de SECOP I agrega {int(graph_metrics.get('historical_contract_count') or 0)} contratos por {compact_money(graph_metrics.get('historical_contract_value') or 0)}.",
    ]
    if graph_metrics.get("top_historical_buyers"):
        findings.append(
            "Los compradores históricos más repetidos en esta muestra son "
            + ", ".join(graph_metrics["top_historical_buyers"])
            + "."
        )
    if official_names:
        findings.append(
            "Además el actor dispara un cruce de control societario con registros públicos de empleo o cargo: "
            + ", ".join(official_names[:3])
            + "."
        )
    if int(graph_metrics.get("archive_contract_count") or 0) > 0:
        findings.append(
            f"El expediente SECOP II agrega {int(graph_metrics.get('archive_contract_count') or 0)} contrato(s) con {int(graph_metrics.get('archive_document_total') or 0)} referencia(s) documentales públicas."
        )
        archive_parts: list[str] = []
        if int(graph_metrics.get("archive_supervision_contract_count") or 0) > 0:
            archive_parts.append(
                f"supervisión en {int(graph_metrics.get('archive_supervision_contract_count') or 0)}"
            )
        if int(graph_metrics.get("archive_start_record_contract_count") or 0) > 0:
            archive_parts.append(
                f"acta de inicio en {int(graph_metrics.get('archive_start_record_contract_count') or 0)}"
            )
        if int(graph_metrics.get("archive_payment_contract_count") or 0) > 0:
            archive_parts.append(
                f"soportes de pago en {int(graph_metrics.get('archive_payment_contract_count') or 0)}"
            )
        if archive_parts:
            findings.append("Dentro del archivo público aparecen " + ", ".join(archive_parts) + " contrato(s).")
        if archive_example := format_archive_example(graph_metrics):
            findings.append(f"Un expediente visible en esta muestra es {archive_example}.")

    metrics = {
        "sanction_count": company.get("sanction_count") or 0,
        "contract_count": company.get("contract_count") or 0,
        "contract_value": company.get("contract_value") or 0.0,
        "sanctioned_still_receiving_contract_count": company.get("sanctioned_still_receiving_contract_count") or 0,
        "sanctioned_still_receiving_total": company.get("sanctioned_still_receiving_total") or 0.0,
        "historical_contract_count": graph_metrics.get("historical_contract_count") or 0,
        "historical_contract_value": graph_metrics.get("historical_contract_value") or 0.0,
        "historical_with_origin_count": graph_metrics.get("historical_with_origin_count") or 0,
        "archive_contract_count": graph_metrics.get("archive_contract_count") or 0,
        "archive_document_total": graph_metrics.get("archive_document_total") or 0,
        "archive_supervision_contract_count": graph_metrics.get("archive_supervision_contract_count") or 0,
        "archive_supervision_document_total": graph_metrics.get("archive_supervision_document_total") or 0,
        "archive_payment_contract_count": graph_metrics.get("archive_payment_contract_count") or 0,
        "archive_payment_document_total": graph_metrics.get("archive_payment_document_total") or 0,
        "archive_start_record_document_total": graph_metrics.get("archive_start_record_document_total") or 0,
        "official_officer_count": company.get("official_officer_count") or 0,
    }
    tags = [format_signal_label(signal) for signal in extract_alert_types(company)[:5]]
    return {
        "slug": f"{slugify(company_name)}-{document_id or 'company'}-historical-sanction-stack",
        "title": f"{company_name}: sanciones con contratación histórica activa",
        "category": "proveedor_sancionado",
        "status": "generated_lead",
        "entity_id": company.get("entity_id"),
        "entity_type": "company",
        "subject_name": company_name,
        "subject_ref": document_id or None,
        "summary": "Señal automática basada en sanciones, continuidad contractual y expansión histórica SECOP I sobre el mismo proveedor.",
        "why_it_matters": "Sirve para priorizar revisión manual donde coinciden antecedente sancionatorio, contratación posterior o concurrente y trayectoria contractual histórica con compradores públicos identificables.",
        "findings": findings,
        "evidence": build_metrics_evidence(
            metrics,
            [
                "sanction_count",
                "contract_count",
                "contract_value",
                "sanctioned_still_receiving_contract_count",
                "sanctioned_still_receiving_total",
                "historical_contract_count",
                "historical_contract_value",
                "historical_with_origin_count",
                "archive_contract_count",
                "archive_document_total",
                "archive_supervision_contract_count",
                "archive_supervision_document_total",
                "archive_payment_contract_count",
                "archive_payment_document_total",
                "archive_start_record_document_total",
                "official_officer_count",
            ],
        ),
        "verified_open_data": [
            f"Contratos históricos SECOP I detectados · {int(graph_metrics.get('historical_contract_count') or 0)} adjudicaciones con NIT exacto.",
            f"Ventana sancionatoria activa en el lote actual · {int(company.get('sanctioned_still_receiving_contract_count') or 0)} contrato(s) señalados.",
            *(
                [
                    f"Archivo SECOP II conectado · {int(graph_metrics.get('archive_contract_count') or 0)} contrato(s) con soporte documental público."
                ]
                if int(graph_metrics.get("archive_contract_count") or 0) > 0
                else []
            ),
        ],
        "open_questions": [
            "Expediente sancionatorio exacto · abrir la resolución y verificar si la restricción aplicaba al periodo de adjudicación o ejecución detectado.",
            "Continuidad contractual · revisar si hubo cesiones, excepciones o cambios de objeto que expliquen la permanencia del actor en contratación pública.",
        ],
        "tags": tags,
        "public_sources": dedupe_strings(
            [
                *generated_public_sources(set(extract_alert_types(company))),
                *(
                    [DATASET_URLS["secop_document_archives"]]
                    if int(graph_metrics.get("archive_contract_count") or 0) > 0
                    else []
                ),
            ]
        ),
        "graph": graph,
    }


def build_generated_official_overlap_investigation(
    company: dict[str, Any],
    graph: dict[str, Any] | None,
    graph_metrics: dict[str, Any],
) -> dict[str, Any]:
    company_name = str(company.get("name") or "").strip()
    document_id = str(company.get("document_id") or "").strip()
    official_names = list(company.get("official_names") or [])
    findings = [
        f"El actor concentra {int(company.get('contract_count') or 0)} contratos por {compact_money(company.get('contract_value') or 0)} en el lote actual.",
        f"La expansión histórica SECOP I agrega {int(graph_metrics.get('historical_contract_count') or 0)} contratos por {compact_money(graph_metrics.get('historical_contract_value') or 0)}.",
    ]
    if official_names:
        findings.append(
            "El cruce societario con empleo o cargo público cae sobre "
            + ", ".join(official_names[:3])
            + "."
        )
    if int(company.get("official_role_count") or 0) > 0 or int(company.get("signal_types") or 0) > 0:
        findings.append(
            f"El mismo actor dispara {int(company.get('signal_types') or 0)} familia(s) de señal, incluyendo traslape oficial y exposición en cargos sensibles."
        )
    if graph_metrics.get("top_historical_buyers"):
        findings.append(
            "Los compradores históricos más repetidos en esta muestra son "
            + ", ".join(graph_metrics["top_historical_buyers"])
            + "."
        )
    if int(graph_metrics.get("archive_contract_count") or 0) > 0:
        findings.append(
            f"El expediente SECOP II agrega {int(graph_metrics.get('archive_contract_count') or 0)} contrato(s) con {int(graph_metrics.get('archive_document_total') or 0)} referencia(s) documentales públicas."
        )
        archive_parts: list[str] = []
        if int(graph_metrics.get("archive_supervision_contract_count") or 0) > 0:
            archive_parts.append(
                f"supervisión en {int(graph_metrics.get('archive_supervision_contract_count') or 0)}"
            )
        if int(graph_metrics.get("archive_assignment_contract_count") or 0) > 0:
            archive_parts.append(
                f"designación o delegación en {int(graph_metrics.get('archive_assignment_contract_count') or 0)}"
            )
        if archive_parts:
            findings.append("Dentro del archivo público aparecen " + ", ".join(archive_parts) + " contrato(s).")
        if archive_example := format_archive_example(graph_metrics):
            findings.append(f"Un expediente visible en esta muestra es {archive_example}.")

    metrics = {
        "contract_count": company.get("contract_count") or 0,
        "contract_value": company.get("contract_value") or 0.0,
        "official_officer_count": company.get("official_officer_count") or 0,
        "official_role_count": company.get("official_role_count") or 0,
        "historical_contract_count": graph_metrics.get("historical_contract_count") or 0,
        "historical_contract_value": graph_metrics.get("historical_contract_value") or 0.0,
        "historical_with_origin_count": graph_metrics.get("historical_with_origin_count") or 0,
        "archive_contract_count": graph_metrics.get("archive_contract_count") or 0,
        "archive_document_total": graph_metrics.get("archive_document_total") or 0,
        "archive_supervision_contract_count": graph_metrics.get("archive_supervision_contract_count") or 0,
        "archive_supervision_document_total": graph_metrics.get("archive_supervision_document_total") or 0,
        "archive_assignment_contract_count": graph_metrics.get("archive_assignment_contract_count") or 0,
        "archive_assignment_document_total": graph_metrics.get("archive_assignment_document_total") or 0,
    }
    if int(company.get("buyer_count") or 0) > 0:
        metrics["buyer_count"] = company.get("buyer_count") or 0

    return {
        "slug": f"{slugify(company_name)}-{document_id or 'company'}-official-overlap",
        "title": f"{company_name}: traslape oficial sobre proveedor privado",
        "category": "captura_contractual",
        "status": "generated_lead",
        "entity_id": company.get("entity_id"),
        "entity_type": "company",
        "subject_name": company_name,
        "subject_ref": document_id or None,
        "summary": "Señal automática basada en un proveedor privado con control societario superpuesto a cargo o salario público y trayectoria contractual histórica.",
        "why_it_matters": "No demuestra por sí solo una incompatibilidad ilegal, pero prioriza actores donde conviene revisar fechas, funciones públicas y el alcance real del control societario.",
        "findings": findings,
        "evidence": build_metrics_evidence(
            metrics,
            [
                "contract_count",
                "contract_value",
                "buyer_count",
                "official_officer_count",
                "official_role_count",
                "historical_contract_count",
                "historical_contract_value",
                "historical_with_origin_count",
                "archive_contract_count",
                "archive_document_total",
                "archive_supervision_contract_count",
                "archive_supervision_document_total",
                "archive_assignment_contract_count",
                "archive_assignment_document_total",
            ],
        ),
        "verified_open_data": [
            f"Cruce societario/oficial · {int(company.get('official_officer_count') or 0)} persona(s) vinculadas al proveedor también aparecen en registros públicos.",
            f"Compradores históricos visibles · {', '.join(graph_metrics.get('top_historical_buyers') or []) or 'sin compradores destacados en el recorte'}",
            *(
                [
                    f"Archivo SECOP II conectado · {int(graph_metrics.get('archive_contract_count') or 0)} contrato(s) con soporte documental público."
                ]
                if int(graph_metrics.get("archive_contract_count") or 0) > 0
                else []
            ),
        ],
        "open_questions": [
            "Temporalidad del vínculo · verificar si la persona estaba en cargo o nómina pública al mismo tiempo que representaba o controlaba al proveedor.",
            "Alcance funcional · revisar si tenía funciones de supervisión, contratación u ordenación del gasto frente a compradores de la misma red de relaciones.",
        ],
        "tags": [format_signal_label(signal) for signal in extract_alert_types(company)[:5]],
        "public_sources": dedupe_strings(
            [
                *generated_public_sources(set(extract_alert_types(company))),
                *(
                    [DATASET_URLS["secop_document_archives"]]
                    if int(graph_metrics.get("archive_contract_count") or 0) > 0
                    else []
                ),
            ]
        ),
        "graph": graph,
    }


def build_generated_investigations(
    api_base: str,
    companies: list[dict[str, Any]],
    people: list[dict[str, Any]],
    validation_cases: list[dict[str, Any]],
    graph_lookup: Callable[[str], dict[str, Any] | None],
) -> list[dict[str, Any]]:
    validated_refs = {normalize_ref(case.get("entity_ref")) for case in validation_cases}
    used_refs: set[str] = set()
    investigations: list[dict[str, Any]] = []
    graph_context_cache: dict[tuple[str, bool], tuple[dict[str, Any], dict[str, Any]]] = {}

    def get_graph_context(
        entity_id: str,
        *,
        include_neighbor_contracts: bool = False,
    ) -> tuple[dict[str, Any], dict[str, Any]] | None:
        if not entity_id:
            return None
        cache_key = (entity_id, include_neighbor_contracts)
        cached = graph_context_cache.get(cache_key)
        if cached is not None:
            return cached
        graph = graph_lookup(entity_id)
        if graph is None:
            return None
        raw_graph = fetch_full_graph_payload(api_base, entity_id, depth=2) or graph
        graph_metrics = summarize_company_graph(
            raw_graph,
            include_neighbor_contracts=include_neighbor_contracts,
        )
        cached = (graph, graph_metrics)
        graph_context_cache[cache_key] = cached
        return cached

    def sanction_sort_key(row: dict[str, Any], graph_metrics: dict[str, Any]) -> tuple[float, ...]:
        return (
            float(row.get("sanctioned_still_receiving_contract_count") or 0),
            float(row.get("sanction_count") or 0),
            *[float(value) for value in archive_priority_tuple(graph_metrics)],
            float(row.get("sanctioned_still_receiving_total") or 0),
            float(row.get("contract_value") or 0),
            float(row.get("suspicion_score") or 0),
        )

    def overlap_sort_key(row: dict[str, Any], graph_metrics: dict[str, Any]) -> tuple[float, ...]:
        return (
            float(row.get("official_officer_count") or 0),
            float(row.get("official_role_count") or 0),
            float(row.get("execution_gap_contract_count") or 0),
            *[float(value) for value in archive_priority_tuple(graph_metrics)],
            float(row.get("contract_value") or 0),
            float(row.get("suspicion_score") or 0),
        )

    def person_sort_key(row: dict[str, Any], graph_metrics: dict[str, Any]) -> tuple[float, ...]:
        return (
            float(row.get("payment_supervision_risk_contract_count") or 0),
            float(row.get("payment_supervision_discrepancy_contract_count") or 0),
            float(row.get("payment_supervision_pending_contract_count") or 0),
            float(row.get("office_count") or 0),
            *[float(value) for value in archive_priority_tuple(graph_metrics)],
            float(row.get("payment_supervision_contract_value") or 0),
            float(row.get("suspicion_score") or 0),
        )

    sanction_contexts: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for row in companies:
        if entity_ref(row) in validated_refs:
            continue
        if not {
            "sanctioned_supplier_record",
            "sanctioned_still_receiving",
        }.issubset(set(extract_alert_types(row))):
            continue
        if int(row.get("contract_count") or 0) < 10:
            continue
        entity_id = str(row.get("entity_id") or "")
        context = get_graph_context(entity_id)
        if context is None:
            continue
        graph, graph_metrics = context
        if int(graph_metrics.get("archive_contract_count") or 0) <= 0:
            continue
        sanction_contexts.append((row, graph, graph_metrics))
    sanction_contexts.sort(
        key=lambda item: sanction_sort_key(item[0], item[2]),
        reverse=True,
    )
    for row, graph, graph_metrics in sanction_contexts:
        ref = entity_ref(row)
        if ref in used_refs:
            continue
        investigations.append(build_generated_sanction_stack_investigation(row, graph, graph_metrics))
        used_refs.add(ref)
        if len(investigations) >= 2:
            break

    overlap_contexts: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for row in companies:
        if entity_ref(row) in validated_refs or entity_ref(row) in used_refs:
            continue
        if looks_like_public_entity(str(row.get("name") or "")):
            continue
        if not {
            "public_official_supplier_overlap",
            "sensitive_public_official_supplier_overlap",
        }.issubset(set(extract_alert_types(row))):
            continue
        if int(row.get("contract_count") or 0) < 20:
            continue
        entity_id = str(row.get("entity_id") or "")
        context = get_graph_context(entity_id)
        if context is None:
            continue
        graph, graph_metrics = context
        if int(graph_metrics.get("archive_contract_count") or 0) <= 0:
            continue
        overlap_contexts.append((row, graph, graph_metrics))
    overlap_contexts.sort(
        key=lambda item: overlap_sort_key(item[0], item[2]),
        reverse=True,
    )
    for row, graph, graph_metrics in overlap_contexts:
        investigations.append(build_generated_official_overlap_investigation(row, graph, graph_metrics))
        used_refs.add(entity_ref(row))
        break

    person_contexts: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for row in people:
        ref = entity_ref(row)
        if ref in validated_refs or ref in used_refs:
            continue
        if "payment_supervision_risk_stack" not in set(extract_alert_types(row)):
            continue
        if int(row.get("payment_supervision_risk_contract_count") or 0) <= 0:
            continue
        entity_id = str(row.get("entity_id") or "")
        context = get_graph_context(entity_id, include_neighbor_contracts=True)
        if context is None:
            continue
        graph, graph_metrics = context
        if int(graph_metrics.get("archive_contract_count") or 0) <= 0:
            continue
        person_contexts.append((row, graph, graph_metrics))
    person_contexts.sort(
        key=lambda item: person_sort_key(item[0], item[2]),
        reverse=True,
    )
    for row, graph, graph_metrics in person_contexts:
        investigations.append(build_generated_payment_supervision_archive_investigation(row, graph, graph_metrics))
        used_refs.add(entity_ref(row))
        break

    return investigations[:GENERATED_INVESTIGATION_LIMIT]


def build_generated_payment_supervision_archive_investigation(
    person: dict[str, Any],
    graph: dict[str, Any] | None,
    graph_metrics: dict[str, Any],
) -> dict[str, Any]:
    person_name = str(person.get("name") or "").strip()
    document_id = str(person.get("document_id") or "").strip()
    findings = [
        f"La persona figura como supervisor(a) de pago en {int(person.get('payment_supervision_count') or 0)} contrato(s) sobre {int(person.get('payment_supervision_company_count') or 0)} proveedor(es), por {compact_money(person.get('payment_supervision_contract_value') or 0)}.",
        f"En esa muestra ya hay {int(person.get('payment_supervision_risk_contract_count') or 0)} contrato(s) con riesgo y {int(person.get('payment_supervision_pending_contract_count') or 0)} con pagos pendientes.",
    ]
    if int(person.get("payment_supervision_discrepancy_contract_count") or 0) > 0:
        findings.append(
            f"Además se detectan {int(person.get('payment_supervision_discrepancy_contract_count') or 0)} contrato(s) supervisados con brecha entre facturación, compromisos o ejecución."
        )
    offices = list(person.get("offices") or [])
    if offices:
        findings.append(
            "El mismo actor aparece en registros públicos de cargo o nómina como "
            + ", ".join(offices[:3])
            + "."
        )
    if int(graph_metrics.get("archive_contract_count") or 0) > 0:
        findings.append(
            f"El archivo SECOP II agrega {int(graph_metrics.get('archive_contract_count') or 0)} contrato(s) con {int(graph_metrics.get('archive_document_total') or 0)} referencia(s) documentales públicas."
        )
        archive_parts: list[str] = []
        if int(graph_metrics.get("archive_supervision_contract_count") or 0) > 0:
            archive_parts.append(
                f"supervisión en {int(graph_metrics.get('archive_supervision_contract_count') or 0)}"
            )
        if int(graph_metrics.get("archive_payment_contract_count") or 0) > 0:
            archive_parts.append(
                f"soportes de pago en {int(graph_metrics.get('archive_payment_contract_count') or 0)}"
            )
        if int(graph_metrics.get("archive_assignment_contract_count") or 0) > 0:
            archive_parts.append(
                f"designación o delegación en {int(graph_metrics.get('archive_assignment_contract_count') or 0)}"
            )
        if archive_parts:
            findings.append("Dentro del archivo público aparecen " + ", ".join(archive_parts) + " contrato(s).")
        if archive_example := format_archive_example(graph_metrics):
            findings.append(f"Un expediente visible en esta muestra es {archive_example}.")

    metrics = {
        "payment_supervision_count": person.get("payment_supervision_count") or 0,
        "payment_supervision_company_count": person.get("payment_supervision_company_count") or 0,
        "payment_supervision_contract_value": person.get("payment_supervision_contract_value") or 0.0,
        "payment_supervision_risk_contract_count": person.get("payment_supervision_risk_contract_count") or 0,
        "payment_supervision_discrepancy_contract_count": person.get("payment_supervision_discrepancy_contract_count") or 0,
        "payment_supervision_pending_contract_count": person.get("payment_supervision_pending_contract_count") or 0,
        "payment_supervision_suspension_contract_count": person.get("payment_supervision_suspension_contract_count") or 0,
        "office_count": person.get("office_count") or 0,
        "archive_contract_count": graph_metrics.get("archive_contract_count") or 0,
        "archive_document_total": graph_metrics.get("archive_document_total") or 0,
        "archive_supervision_contract_count": graph_metrics.get("archive_supervision_contract_count") or 0,
        "archive_supervision_document_total": graph_metrics.get("archive_supervision_document_total") or 0,
        "archive_payment_contract_count": graph_metrics.get("archive_payment_contract_count") or 0,
        "archive_payment_document_total": graph_metrics.get("archive_payment_document_total") or 0,
        "archive_assignment_contract_count": graph_metrics.get("archive_assignment_contract_count") or 0,
        "archive_assignment_document_total": graph_metrics.get("archive_assignment_document_total") or 0,
    }
    tags = [format_signal_label(signal) for signal in extract_alert_types(person)[:5]]
    return {
        "slug": f"{slugify(person_name)}-{document_id or 'person'}-payment-supervision-archive-stack",
        "title": f"{person_name}: supervisión de pagos con expediente SECOP visible",
        "category": "supervision_pago_documental",
        "status": "generated_lead",
        "entity_id": person.get("entity_id"),
        "entity_type": "person",
        "subject_name": person_name,
        "subject_ref": document_id or None,
        "summary": "Señal automática basada en supervisión de pagos sobre contratos con alertas duras y expediente documental público ya disponible en SECOP II.",
        "why_it_matters": "Prioriza revisión manual donde la misma persona aparece aprobando o supervisando pagos sobre contratos con señales de riesgo y además ya existen soportes públicos para revisar el expediente sin depender de filtraciones.",
        "findings": findings,
        "evidence": build_metrics_evidence(
            metrics,
            [
                "payment_supervision_count",
                "payment_supervision_company_count",
                "payment_supervision_contract_value",
                "payment_supervision_risk_contract_count",
                "payment_supervision_discrepancy_contract_count",
                "payment_supervision_pending_contract_count",
                "payment_supervision_suspension_contract_count",
                "office_count",
                "archive_contract_count",
                "archive_document_total",
                "archive_supervision_contract_count",
                "archive_supervision_document_total",
                "archive_payment_contract_count",
                "archive_payment_document_total",
                "archive_assignment_contract_count",
                "archive_assignment_document_total",
            ],
        ),
        "verified_open_data": [
            f"Supervisión de pagos en SECOP II · {int(person.get('payment_supervision_count') or 0)} contrato(s) y {int(person.get('payment_supervision_risk_contract_count') or 0)} con señal de riesgo.",
            f"Archivo SECOP II conectado · {int(graph_metrics.get('archive_contract_count') or 0)} contrato(s) con soporte documental público.",
        ],
        "open_questions": [
            "Alcance funcional · verificar si la persona solo apoyaba el trámite o si tenía facultad real para aprobar hitos, pagos o recibo a satisfacción.",
            "Soportes materiales · abrir actas, estudios previos, designaciones y soportes de pago para confirmar si el bien o servicio ya estaba ejecutado cuando se habilitó el pago.",
        ],
        "tags": tags,
        "public_sources": dedupe_strings(
            [
                *generated_public_sources(set(extract_alert_types(person))),
                DATASET_URLS["secop_document_archives"],
            ]
        ),
        "graph": graph,
    }


def build_investigations(
    api_base: str,
    validation_cases: list[dict[str, Any]],
    companies: list[dict[str, Any]],
    people: list[dict[str, Any]],
    graph_lookup: Callable[[str], dict[str, Any] | None],
) -> list[dict[str, Any]]:
    by_case_id = {str(case.get("case_id") or ""): case for case in validation_cases}
    investigations: list[dict[str, Any]] = []
    ungrd_bundle = load_ungrd_structured_evidence()

    if (san_jose := by_case_id.get("san_jose_education_control_capture")):
        if (investigation := build_san_jose_investigation(api_base, san_jose)) is not None:
            investigations.append(investigation)

    stalled = by_case_id.get("fondecun_stalled_work")
    overlap = by_case_id.get("fondecun_official_overlap")
    if stalled and overlap:
        investigations.append(
            build_fondecun_investigation(
                stalled,
                overlap,
                graph_lookup(str(stalled.get("entity_id") or "")),
            )
        )

    if (maybe_case := by_case_id.get("suministros_maybe_sanctioned_still_receiving")):
        investigations.append(
            build_sanction_window_investigation(
                maybe_case,
                graph_lookup(str(maybe_case.get("entity_id") or "")),
            )
        )

    if (vivian_case := by_case_id.get("vivian_moreno_candidate_supplier")):
        investigations.append(
            build_vivian_investigation(
                vivian_case,
                graph_lookup(str(vivian_case.get("entity_id") or "")),
            )
        )

    egobus = by_case_id.get("egobus_sanctioned_supplier")
    coobus = by_case_id.get("coobus_sanctioned_supplier")
    if egobus and coobus:
        investigations.append(
            build_transmilenio_operator_investigation(
                egobus,
                coobus,
                graph_lookup(str(egobus.get("entity_id") or "")),
            )
        )

    for case_id in (
        "alejandro_ospina_coll_bulletin_exposure",
        "federico_garcia_arbelaez_bulletin_exposure",
        "jaime_jose_garces_garcia_bulletin_record",
        "olmedo_lopez_ungrd_bulletin_record",
        "sneyder_pinilla_ungrd_bulletin_record",
        "carlos_ramon_gonzalez_ungrd_bulletin_record",
        "luis_carlos_barreto_ungrd_bulletin_record",
        "sandra_ortiz_ungrd_bulletin_record",
        "maria_alejandra_benavides_ungrd_bulletin_record",
    ):
        if (bulletin_case := by_case_id.get(case_id)):
            investigations.append(
                build_official_bulletin_investigation(
                    bulletin_case,
                    graph_lookup(str(bulletin_case.get("entity_id") or "")),
                    ungrd_bundle,
                )
            )

    investigations.extend(
        build_generated_investigations(
            api_base,
            companies,
            people,
            validation_cases,
            graph_lookup,
        )
    )

    return investigations[:INVESTIGATION_LIMIT]


def build_pack(api_base: str) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    progress("fetching meta stats")
    stats = fetch_json(api_base, "/api/v1/meta/stats")
    progress("fetching validation cases")
    validation = fetch_json(api_base, "/api/v1/meta/validation/known-cases")
    progress("fetching people watchlist")
    people_payload = fetch_json(api_base, f"/api/v1/meta/watchlist/people?limit={PEOPLE_LIMIT}")
    progress("fetching company watchlist")
    companies_payload = fetch_json(api_base, f"/api/v1/meta/watchlist/companies?limit={COMPANY_LIMIT}")
    progress("fetching buyer watchlist")
    buyers_payload = fetch_json(api_base, f"/api/v1/meta/watchlist/buyers?limit={BUYER_LIMIT}")
    progress("fetching territory watchlist")
    territories_payload = fetch_json(api_base, f"/api/v1/meta/watchlist/territories?limit={TERRITORY_LIMIT}")

    validation_cases = list(validation.get("cases") or [])
    validation_index = build_validation_index(validation_cases)
    companies = list(companies_payload.get("companies") or [])
    people = list(people_payload.get("people") or [])
    company_rows_by_id = {
        str(row.get("entity_id") or "").strip(): row
        for row in companies
        if str(row.get("entity_id") or "").strip()
    }
    person_rows_by_id = {
        str(row.get("entity_id") or "").strip(): row
        for row in people
        if str(row.get("entity_id") or "").strip()
    }

    selected_companies = select_diverse_rows(companies, FEATURED_COMPANY_LIMIT, validation_index)
    selected_people = select_diverse_rows(
        [person for person in people if should_feature_person(person)],
        FEATURED_PERSON_LIMIT,
        validation_index,
    )

    featured_companies = [
        feature
        for company in selected_companies
        if (feature := build_company_feature(api_base, company, validation_index)) is not None
    ]
    progress(f"built {len(featured_companies)} featured companies")
    featured_people = [
        build_person_feature(person, validation_index)
        for person in selected_people
    ]
    progress(f"built {len(featured_people)} featured people")
    practice_groups = build_practice_groups(companies, people, validation_index)
    progress(f"built {len(practice_groups)} practice groups")

    graph_cache: dict[str, dict[str, Any] | None] = {}

    def attach_live_graph(entity_id: str | None) -> dict[str, Any] | None:
        if not entity_id:
            return None
        if entity_id not in graph_cache:
            graph_cache[entity_id] = fetch_graph_payload(api_base, entity_id)
        return graph_cache[entity_id]

    def attach_case_graph(entity_type: str, row: dict[str, Any], *, prefer_live: bool = False) -> dict[str, Any] | None:
        entity_id = str(row.get("entity_id") or "").strip()
        if prefer_live:
            graph = attach_live_graph(entity_id)
            if graph is not None:
                return graph
        return build_watchlist_evidence_graph(entity_type, row)

    def attach_validation_graph(case: dict[str, Any]) -> dict[str, Any] | None:
        entity_id = str(case.get("entity_id") or "").strip()
        if not entity_id:
            return None
        if str(case.get("case_id") or "") == "san_jose_education_control_capture":
            cache_key = f"{entity_id}:depth2"
            if cache_key not in graph_cache:
                graph_cache[cache_key] = fetch_graph_payload(api_base, entity_id, depth=2)
            return graph_cache[cache_key]
        return attach_live_graph(entity_id)

    for feature in featured_companies:
        company_row = company_rows_by_id.get(str(feature.get("entity_id") or "").strip(), feature)
        feature["graph"] = attach_case_graph(
            "company",
            company_row,
            prefer_live=bool(feature.get("matched_validation_titles")),
        )
    for feature in featured_people:
        person_row = person_rows_by_id.get(str(feature.get("entity_id") or "").strip(), feature)
        feature["graph"] = attach_case_graph(
            "person",
            person_row,
            prefer_live=bool(feature.get("matched_validation_titles")),
        )

    case_payloads: dict[str, dict[str, Any]] = {}

    def materialize_watchlist_cases(
        entity_type: str,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        materialized_rows: list[dict[str, Any]] = []
        for index, row in enumerate(rows, start=1):
            row_copy = dict(row)
            graph = attach_case_graph(
                entity_type,
                row_copy,
                prefer_live=bool(entity_validation_titles(row_copy, validation_index)),
            )
            case_payload = build_materialized_case(
                entity_type,
                row_copy,
                validation_index,
                graph,
            )
            if case_payload is not None:
                filename = safe_case_filename(entity_type, str(row_copy.get("entity_id") or "").strip())
                case_payloads[filename] = case_payload
                row_copy["case_file"] = f"/data/cases/{filename}"
            else:
                row_copy["case_file"] = None
            materialized_rows.append(row_copy)
            if index == 1 or index % 100 == 0 or index == len(rows):
                progress(f"materialized {entity_type} cases {index}/{len(rows)}")
        return materialized_rows

    progress("materializing company cases")
    materialized_companies = materialize_watchlist_cases("company", companies)
    progress("materializing person cases")
    materialized_people = materialize_watchlist_cases("person", people)

    enriched_validation_cases: list[dict[str, Any]] = []
    for case in validation_cases:
        case_copy = dict(case)
        case_copy["graph"] = attach_validation_graph(case_copy)
        enriched_validation_cases.append(case_copy)

    validation = {
        **validation,
        "cases": enriched_validation_cases,
    }
    investigations = build_investigations(
        api_base,
        enriched_validation_cases,
        companies,
        people,
        attach_live_graph,
    )
    progress(f"built {len(investigations)} investigations")

    san_jose_investigation = next(
        (
            investigation
            for investigation in investigations
            if str(investigation.get("slug") or "") == "san-jose-icaft-network"
        ),
        None,
    )
    if san_jose_investigation is not None:
        for validation_case in validation["cases"]:
            if str(validation_case.get("case_id") or "") != "san_jose_education_control_capture":
                continue
            validation_case["graph"] = san_jose_investigation.get("graph")
            validation_case["summary"] = (
                "Control institucional MEN + alias SECOP + convenios de Apía, con una capa societaria "
                "verificable sobre Business Labs, Andrés Méndez y Mauricio Guevara sin afirmar un puente "
                "oficial abierto que todavía no está documentado."
            )
            validation_case["public_sources"] = list(san_jose_investigation.get("public_sources") or [])
            break

    return {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pack_type": "materialized_real_results",
        "scope_note": (
            "Lote guardado desde el grafo público en vivo. Sin datos sintéticos; "
            "solo cruces reales materializados para consulta y priorización."
        ),
        "stats": stats,
        "validation": validation,
        "summary": {
            "validation_match_rate": (
                (validation.get("matched") or 0) / validation.get("total")
                if validation.get("total")
                else 0.0
            ),
            "featured_company_count": len(featured_companies),
            "featured_person_count": len(featured_people),
            "company_watchlist_count": len(materialized_companies),
            "people_watchlist_count": len(materialized_people),
            "buyer_watchlist_count": len(buyers_payload.get("buyers") or []),
            "territory_watchlist_count": len(territories_payload.get("territories") or []),
        },
        "practice_summary": build_practice_summary(practice_groups),
        "practice_groups": practice_groups,
        "investigations": investigations,
        "featured_companies": featured_companies,
        "featured_people": featured_people,
        "watchlists": {
            "companies": materialized_companies,
            "people": materialized_people,
            "buyers": buyers_payload.get("buyers") or [],
            "territories": territories_payload.get("territories") or [],
        },
    }, case_payloads


def write_payload(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    clean_payload = sanitize_payload(payload)
    path.write_text(json.dumps(clean_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_case_payloads(payloads: dict[str, dict[str, Any]], directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for stale_file in directory.glob("*.json"):
        stale_file.unlink()
    for filename, payload in payloads.items():
        clean_payload = sanitize_payload(payload)
        (directory / filename).write_text(
            json.dumps(clean_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize real-results pack from live API")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--mirror", default=DEFAULT_MIRROR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    progress("starting pack build")
    payload, case_payloads = build_pack(args.api_base)
    progress("writing payload")
    write_payload(payload, Path(args.output))
    write_case_payloads(case_payloads, Path(args.output).resolve().parent / "cases")
    if args.mirror:
        write_payload(payload, Path(args.mirror))
        write_case_payloads(case_payloads, Path(args.mirror).resolve().parent / "cases")
    progress("done")
    print(
        json.dumps(
            {
                "output": args.output,
                "mirror": args.mirror,
                "materialized_case_files": len(case_payloads),
                "investigations": len(payload.get("investigations") or []),
                "featured_companies": len(payload["featured_companies"]),
                "featured_people": len(payload["featured_people"]),
                "validation_matched": payload["validation"]["matched"],
                "validation_total": payload["validation"]["total"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
