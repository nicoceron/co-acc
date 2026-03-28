#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
import urllib.parse
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
BUNDLE_DIR_DEFAULT = REPO_ROOT / "audit-results" / "investigations" / "transmilenio-finance-2026-03-28"

PDFTOTEXT_BIN = "/opt/homebrew/bin/pdftotext"
PDFINFO_BIN = "/opt/homebrew/bin/pdfinfo"

PROCESS_ORDER = [
    "Adquisición de Bienes y Servicios",
    "Desarrollo Estratégico",
    "Evaluación y Mejoramiento de la Gestión",
    "Gestión de Asuntos Disciplinarios",
    "Gestión de Grupos de Interés",
    "Gestión de Información Financiera y Contable",
    "Gestión de Mercadeo",
    "Gestión de Servicios Logísticos",
    "Gestión de Talento Humano",
    "Gestión de TIC",
    "Gestión Económica de los Agentes del Sistema",
    "Gestión Jurídica",
    "Monitoreo Integral a la Operación del SITP",
    "Planeación del SITP",
    "Supervisión y Control de la Operación del SITP",
]


def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def resolve_bundle_dir(raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def read_manifest(bundle_dir: Path) -> dict[str, Any]:
    return json.loads((bundle_dir / "manifest.json").read_text(encoding="utf-8"))


def pdf_to_text(path: Path) -> str:
    txt_path = path.with_suffix(".txt")
    if txt_path.exists():
        return txt_path.read_text(encoding="utf-8", errors="ignore")

    result = subprocess.run(
        [PDFTOTEXT_BIN, str(path), "-"],
        check=True,
        capture_output=True,
        text=True,
    )
    txt_path.write_text(result.stdout, encoding="utf-8")
    return result.stdout


def pdf_page_count(path: Path) -> int:
    try:
        result = subprocess.run(
            [PDFINFO_BIN, str(path)],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:  # noqa: BLE001
        return 0
    match = re.search(r"^Pages:\s+(\d+)\s*$", result.stdout, re.MULTILINE)
    return int(match.group(1)) if match else 0


def parse_plain_amount(raw: str) -> int:
    cleaned = raw.strip()
    if "," in cleaned:
        cleaned = cleaned.split(",", 1)[0]
    digits = re.sub(r"[^\d]", "", cleaned)
    return int(digits) if digits else 0


def parse_signed_plain_amount(raw: str) -> int:
    cleaned = raw.strip()
    negative = cleaned.startswith("-")
    value = parse_plain_amount(cleaned)
    return -value if negative and value else value


def parse_scaled_amount(raw: str, scale: str) -> int:
    cleaned = raw.strip()
    if "," in cleaned:
        numeric = float(cleaned.replace(".", "").replace(",", "."))
    elif "." in cleaned:
        dot_count = cleaned.count(".")
        whole, fractional = cleaned.rsplit(".", 1)
        if dot_count > 1 and len(fractional) in (1, 2):
            numeric = float(f"{whole.replace('.', '')}.{fractional}")
        elif len(fractional) == 3:
            numeric = float(cleaned.replace(".", ""))
        else:
            numeric = float(cleaned)
    else:
        numeric = float(cleaned)
    lowered = scale.lower()
    if "billon" in lowered:
        return int(round(numeric * 1_000_000_000_000))
    if "mil millones" in lowered:
        return int(round(numeric * 1_000_000_000))
    if "millones" in lowered:
        return int(round(numeric * 1_000_000))
    return int(round(numeric))


def extract_excerpt(text: str, pattern: str, radius: int = 220) -> str | None:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    start = max(match.start() - radius, 0)
    end = min(match.end() + radius, len(text))
    return normalize_text(text[start:end])


def extract_document_year(*parts: str) -> int | None:
    for part in parts:
        decoded = urllib.parse.unquote(part)
        match = re.search(r"\b(20\d{2})\b", decoded)
        if match:
            year = int(match.group(1))
            if 2020 <= year <= 2035:
                return year
    return None


def classify_document(label: str, filename: str) -> str:
    lowered = f"{label} {filename}".lower()
    if "oci-2024-053" in lowered:
        return "auditoria_financiera_contable"
    if "oci-2025-020" in lowered:
        return "seguimiento_pt ep".replace(" ", "")
    if "anexo-16" in lowered or "anexo-5" in lowered or "tesoreria" in lowered or "tesorería" in lowered:
        return "informe_tesoreria"
    if "anexo 14" in lowered or "anexo%2014" in lowered or ("gastos" in lowered and "transmilenio" in lowered):
        return "informe_gastos"
    if "anexo 13" in lowered or "anexo%2013" in lowered or "ingresos de transmilenio" in lowered:
        return "informe_ingresos"
    if "anexo 15" in lowered or "anexo%2015" in lowered or "modificaciones presupuestales" in lowered:
        return "informe_modificaciones_presupuestales"
    if "anexo-4" in lowered or "gestion presupuestal" in lowered or "gestión presupuestal" in lowered:
        return "gestion_presupuestal"
    if "gestion-y-sostenibilidad" in lowered or "informe de gestión" in lowered or "informe de gestion" in lowered:
        return "informe_gestion"
    return "documento_publico"


def load_documents(bundle_dir: Path, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for item in manifest.get("downloaded_documents") or []:
        if not isinstance(item, dict) or not item.get("downloaded"):
            continue
        relative = Path(str(item.get("saved_path") or ""))
        path = (bundle_dir / relative).resolve()
        raw_text = pdf_to_text(path)
        label = str(item.get("label") or "").strip()
        documents.append(
            {
                "label": label,
                "filename": path.name,
                "doc_type": classify_document(label, path.name),
                "year": extract_document_year(label, path.name, raw_text[:800]),
                "source_url": str(item.get("url") or "").strip(),
                "saved_path": str(path),
                "bytes": int(item.get("bytes") or 0),
                "page_count": pdf_page_count(path),
                "text_length": len(raw_text),
                "excerpt": normalize_text(raw_text[:2000]),
                "_raw_text": raw_text,
            }
        )
    return documents


def extract_ptep_finance_process_counts(text: str) -> dict[str, int]:
    block_match = re.search(
        r"Total, por proceso\s+(.*?)\s+Fuente: Matriz riesgos de corrupción 2025",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not block_match:
        return {}
    numbers = [int(value) for value in re.findall(r"\b\d+\b", block_match.group(1))]
    if len(numbers) < 32:
        return {}
    risks = numbers[:16]
    controls = numbers[16:32]
    try:
        finance_index = PROCESS_ORDER.index("Gestión de Información Financiera y Contable")
    except ValueError:
        return {}
    return {
        "total_corruption_risk_count": risks[-1],
        "total_corruption_control_count": controls[-1],
        "financial_process_corruption_risk_count": risks[finance_index],
        "financial_process_corruption_control_count": controls[finance_index],
    }


def build_alert(
    signal_id: str,
    title: str,
    severity_score: int,
    summary: str,
    source_document: str,
    excerpt: str | None,
    metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "signal_id": signal_id,
        "title": title,
        "severity_score": severity_score,
        "summary": summary,
        "source_document": source_document,
        "metrics": metrics or {},
        "excerpt": excerpt,
    }


def first_document_of_type(documents: list[dict[str, Any]], doc_type: str) -> dict[str, Any] | None:
    candidates = [document for document in documents if str(document.get("doc_type") or "") == doc_type]
    if not candidates:
        return None
    return max(candidates, key=lambda item: int(item.get("year") or 0))


def extract_treasury_cdt_total(text: str) -> int:
    match = re.search(
        r"dos\s+\(2\)\s+CDT\S*\s+con un valor nominal de \$([0-9\.\,]+)\s+millones",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return 0
    return parse_scaled_amount(match.group(1), "millones")


def extract_budget_modification_total(document: dict[str, Any]) -> int:
    text = str(document.get("_raw_text") or "")
    doc_type = str(document.get("doc_type") or "")
    if doc_type == "informe_gastos":
        match = re.search(
            r"EJECUCIÓN PRESUPUESTAL DE GASTOS.*?GASTOS\s+[0-9\.\,]+\s+(-?[0-9\.\,]+)\s+(-?[0-9\.\,]+)",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if match:
            return abs(parse_signed_plain_amount(match.group(2)))
    if doc_type == "gestion_presupuestal":
        match = re.search(
            r"modificaciones presupuestales en ingresos por valor de\s*\$\s*([0-9\.\,]+)\s+millones",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if match:
            return parse_scaled_amount(match.group(1), "millones")
    return 0


def extract_findings(documents: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    metrics: dict[str, Any] = {}
    alerts: list[dict[str, Any]] = []

    document_years = sorted({int(document.get("year")) for document in documents if isinstance(document.get("year"), int)})
    metrics["finance_document_year_count"] = len(document_years)
    metrics["finance_document_years"] = document_years
    if document_years:
        metrics["finance_document_year_span"] = (max(document_years) - min(document_years)) + 1

    audit_doc = first_document_of_type(documents, "auditoria_financiera_contable")
    ptep_doc = first_document_of_type(documents, "seguimiento_ptep")
    latest_treasury_doc = first_document_of_type(documents, "informe_tesoreria")
    latest_management_doc = first_document_of_type(documents, "informe_gestion")
    treasury_docs = [document for document in documents if str(document.get("doc_type") or "") == "informe_tesoreria"]
    management_docs = [document for document in documents if str(document.get("doc_type") or "") == "informe_gestion"]
    budget_docs = [
        document
        for document in documents
        if str(document.get("doc_type") or "") in {"informe_gastos", "gestion_presupuestal", "informe_modificaciones_presupuestales"}
    ]
    signal_years: set[int] = set()
    treasury_cdt_years: set[int] = set()
    third_party_rule_years: set[int] = set()
    budget_modification_by_year: dict[int, int] = {}

    audit_text = str((audit_doc or {}).get("_raw_text") or "")
    ptep_text = str((ptep_doc or {}).get("_raw_text") or "")
    treasury_text = str((latest_treasury_doc or {}).get("_raw_text") or "")
    management_text = str((latest_management_doc or {}).get("_raw_text") or "")

    if audit_text:
        cheque_match = re.search(
            r"emitieron\s+(\d+)\s+cheques.*?valor total de \$([0-9\.\,]+).*?sellos restrictivos.*?ventanilla",
            audit_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if cheque_match:
            cheque_count = int(cheque_match.group(1))
            cheque_total = parse_plain_amount(cheque_match.group(2))
            metrics["payroll_cheque_exception_count"] = cheque_count
            metrics["payroll_cheque_exception_total"] = cheque_total
            metrics["payroll_teller_window_request_count"] = 1
            if isinstance((audit_doc or {}).get("year"), int):
                signal_years.add(int((audit_doc or {}).get("year")))
            alerts.append(
                build_alert(
                    "payroll_cheque_exception",
                    "Cheques excepcionales de nómina",
                    5,
                    "La auditoría OCI detectó pagos de nómina por cheque a un solo funcionario, con solicitud posterior para levantar sellos restrictivos y reclamar por ventanilla.",
                    str((audit_doc or {}).get("source_url") or ""),
                    extract_excerpt(audit_text, r"emitieron\s+\d+\s+cheques.*?ventanilla"),
                    {
                        "payroll_cheque_exception_count": cheque_count,
                        "payroll_cheque_exception_total": cheque_total,
                    },
                )
            )

        mismatch_match = re.search(
            r"evidenciando una diferencia de \$([0-9\.\,]+)",
            audit_text,
            flags=re.IGNORECASE,
        )
        if mismatch_match:
            mismatch_total = parse_plain_amount(mismatch_match.group(1))
            metrics["treasury_jsp7_difference_total"] = mismatch_total
            if isinstance((audit_doc or {}).get("year"), int):
                signal_years.add(int((audit_doc or {}).get("year")))
            alerts.append(
                build_alert(
                    "treasury_jsp7_gap",
                    "Descuadre entre tesorería y JSP7",
                    5,
                    "La auditoría OCI reportó una diferencia entre el reporte de tesorería y el aplicativo JSP7 en los ingresos con y sin afectación presupuestal.",
                    str((audit_doc or {}).get("source_url") or ""),
                    extract_excerpt(audit_text, r"diferencia de \$[0-9\.\,]+.*?JSP7"),
                    {"treasury_jsp7_difference_total": mismatch_total},
                )
            )

        crp_match = re.search(
            r"se eliminó el\s+(\d{6}-\d+)\s+desde el administrador del aplicativo JSP7",
            audit_text,
            flags=re.IGNORECASE,
        )
        if crp_match:
            metrics["deleted_crp_sequence_count"] = 1
            metrics["deleted_crp_sequences"] = [crp_match.group(1)]
            if isinstance((audit_doc or {}).get("year"), int):
                signal_years.add(int((audit_doc or {}).get("year")))
            alerts.append(
                build_alert(
                    "deleted_crp_sequence",
                    "Consecutivo CRP eliminado desde JSP7",
                    4,
                    "La auditoría OCI registró la eliminación de un consecutivo presupuestal desde el perfil administrador de JSP7, con impacto sobre la integridad de la información.",
                    str((audit_doc or {}).get("source_url") or ""),
                    extract_excerpt(audit_text, r"eliminó el\s+\d{6}-\d+\s+desde el administrador del aplicativo JSP7"),
                    {
                        "deleted_crp_sequence_count": 1,
                        "deleted_crp_sequences": [crp_match.group(1)],
                    },
                )
            )

    if ptep_text:
        risk_counts = extract_ptep_finance_process_counts(ptep_text)
        if risk_counts:
            metrics.update(risk_counts)
            if isinstance((ptep_doc or {}).get("year"), int):
                signal_years.add(int((ptep_doc or {}).get("year")))
            alerts.append(
                build_alert(
                    "financial_process_corruption_risk",
                    "Proceso financiero con riesgo de corrupción vigente",
                    2,
                    "El PTEP 2025 mantiene a Gestión de Información Financiera y Contable dentro del mapa de riesgos de corrupción de la entidad.",
                    str((ptep_doc or {}).get("source_url") or ""),
                    extract_excerpt(ptep_text, r"Seguimiento de los riesgos de corrupción:.*?Gestión de Información Financiera y Contable.*?Fuente: Matriz riesgos de corrupción 2025"),
                    risk_counts,
                )
            )

    if treasury_text:
        cdt_match = re.search(
            r"dos\s+\(2\)\s+CDT\S*\s+con un valor nominal de \$([0-9\.\,]+)\s+millones",
            treasury_text,
            flags=re.IGNORECASE,
        )
        if cdt_match:
            metrics["treasury_cdt_count"] = 2
            metrics["treasury_cdt_nominal_total"] = parse_scaled_amount(cdt_match.group(1), "millones")

        convenio_match = re.search(
            r"se realizaron\s+([0-9\.\,]+)\s+pagos por \$\s*([0-9\.\,]+)\s+mil millones",
            treasury_text,
            flags=re.IGNORECASE,
        )
        if convenio_match:
            metrics["treasury_convenio_payment_count"] = parse_plain_amount(convenio_match.group(1))
            metrics["treasury_convenio_payment_total"] = parse_scaled_amount(convenio_match.group(2), "mil millones")

        vehicle_match = re.search(
            r"Pagos a propietarios de vehículos.*?se realizaron\s+([0-9\.\,]+)\s+pagos\s+\(vehículos\)\s+por un total\s+de \$([0-9\.\,]+)\s+millones",
            treasury_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if vehicle_match:
            metrics["vehicle_owner_payment_count"] = parse_plain_amount(vehicle_match.group(1))
            metrics["vehicle_owner_payment_total"] = parse_scaled_amount(vehicle_match.group(2), "millones")

        pac_match = re.search(
            r"valor de\s+([0-9\.\,]+)\s+mil millones.*?FET \$\s*([0-9\.\,]+)\s+Billones",
            treasury_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if pac_match:
            metrics["pac_district_transfer_total"] = parse_scaled_amount(pac_match.group(1), "mil millones")
            metrics["pac_fet_transfer_total"] = parse_scaled_amount(pac_match.group(2), "billones")

        if re.search(r"órdenes de\s+pago masivas para contratistas", treasury_text, flags=re.IGNORECASE):
            metrics["contractor_mass_payment_order_signal"] = 1
            if isinstance((latest_treasury_doc or {}).get("year"), int):
                signal_years.add(int((latest_treasury_doc or {}).get("year")))
            alerts.append(
                build_alert(
                    "mass_payment_orders",
                    "Órdenes de pago masivas para contratistas",
                    1,
                    "Tesorería reporta que en 2025 entró en operación un flujo de órdenes de pago masivas para contratistas. No es irregular por sí solo, pero aumenta la necesidad de controles transaccionales finos.",
                    str((latest_treasury_doc or {}).get("source_url") or ""),
                    extract_excerpt(treasury_text, r"órdenes de\s+pago masivas para contratistas"),
                    {"contractor_mass_payment_order_signal": 1},
                )
            )

    if management_text:
        management_counts = re.search(
            r"suscripción\s+([0-9\.\,]+)\s+contratos,\s+([0-9\.\,]+)\s+modificaciones,\s+([0-9\.\,]+)\s+novedades contractuales.*?y\s+([0-9\.\,]+)\s+solicitudes de información a\s+proveedores",
            management_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if management_counts:
            metrics["management_contract_count"] = parse_plain_amount(management_counts.group(1))
            metrics["management_contract_modification_count"] = parse_plain_amount(management_counts.group(2))
            metrics["management_contract_novelty_count"] = parse_plain_amount(management_counts.group(3))
            metrics["management_supplier_request_count"] = parse_plain_amount(management_counts.group(4))

        if re.search(r"solicitud de pagos a nombre de terceros", management_text, flags=re.IGNORECASE):
            metrics["third_party_non_holder_rule_signal"] = 1
            if isinstance((latest_management_doc or {}).get("year"), int):
                signal_years.add(int((latest_management_doc or {}).get("year")))
            alerts.append(
                build_alert(
                    "third_party_payment_override_route",
                    "Ruta formal para pagos a terceros no titulares",
                    2,
                    "El informe de gestión recuerda que existe una vía formal para pagar a terceros no titulares del derecho con comunicación autenticada. No prueba desvío, pero sí identifica una excepción operativa sensible.",
                    str((latest_management_doc or {}).get("source_url") or ""),
                    extract_excerpt(management_text, r"solicitud de pagos a nombre de terceros.*?beneficiario"),
                    {"third_party_non_holder_rule_signal": 1},
                )
            )

    for document in treasury_docs:
        year = document.get("year")
        if not isinstance(year, int):
            continue
        text = str(document.get("_raw_text") or "")
        if extract_treasury_cdt_total(text) > 0:
            treasury_cdt_years.add(year)

    for document in management_docs:
        year = document.get("year")
        if not isinstance(year, int):
            continue
        text = str(document.get("_raw_text") or "")
        if re.search(r"solicitud de pagos a nombre de terceros", text, flags=re.IGNORECASE):
            third_party_rule_years.add(year)

    for document in budget_docs:
        year = document.get("year")
        if not isinstance(year, int):
            continue
        total = extract_budget_modification_total(document)
        if total > 0:
            budget_modification_by_year[year] = max(total, budget_modification_by_year.get(year, 0))
            signal_years.add(year)

    if treasury_cdt_years:
        metrics["recurring_treasury_document_year_count"] = len(treasury_cdt_years)
        metrics["recurring_treasury_document_years"] = sorted(treasury_cdt_years)
    if third_party_rule_years:
        metrics["recurring_third_party_payment_rule_year_count"] = len(third_party_rule_years)
        metrics["recurring_third_party_payment_rule_years"] = sorted(third_party_rule_years)
    if budget_modification_by_year:
        metrics["recurring_budget_modification_year_count"] = len(budget_modification_by_year)
        metrics["recurring_budget_modification_years"] = sorted(budget_modification_by_year)
        metrics["recurring_budget_modification_total"] = sum(budget_modification_by_year.values())
    if signal_years:
        metrics["recurring_exception_surface_year_count"] = len(signal_years)
        metrics["recurring_exception_surface_years"] = sorted(signal_years)

    if len(signal_years) >= 2:
        source_document = str((audit_doc or ptep_doc or latest_treasury_doc or latest_management_doc or {}).get("source_url") or "")
        years = sorted(signal_years)
        alerts.append(
            build_alert(
                "multi_period_finance_exception_surface",
                "Persistencia interanual de señales financieras",
                3,
                f"Entre {years[0]} y {years[-1]} los documentos oficiales de TRANSMILENIO repiten señales en tesorería, presupuesto y control interno, lo que sugiere una zona roja recurrente y no un hallazgo aislado.",
                source_document,
                None,
                {
                    "recurring_exception_surface_year_count": len(years),
                    "recurring_exception_surface_years": years,
                },
            )
        )

    if len(budget_modification_by_year) >= 2:
        source_document = ""
        for document in budget_docs:
            source_document = str(document.get("source_url") or "").strip()
            if source_document:
                break
        alerts.append(
            build_alert(
                "recurrent_budget_modifications",
                "Modificaciones presupuestales altas en más de una vigencia",
                2,
                "Los anexos presupuestales oficiales muestran modificaciones agregadas de gran tamaño en más de un año, lo que vuelve prioritario revisar trazabilidad de ajustes, justificaciones y responsables.",
                source_document,
                None,
                {
                    "recurring_budget_modification_year_count": len(budget_modification_by_year),
                    "recurring_budget_modification_total": sum(budget_modification_by_year.values()),
                },
            )
        )

    metrics["document_count"] = len(documents)
    metrics["signal_count"] = len(alerts)
    metrics["priority_score"] = sum(int(alert.get("severity_score") or 0) for alert in alerts)
    return metrics, sorted(alerts, key=lambda item: int(item.get("severity_score") or 0), reverse=True)


def build_summary(metrics: dict[str, Any], alerts: list[dict[str, Any]], documents: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "documents_total": len(documents),
        "alerts_total": len(alerts),
        "high_severity_alerts": sum(1 for item in alerts if int(item.get("severity_score") or 0) >= 4),
        "priority_score": int(metrics.get("priority_score") or 0),
        "top_signals": [str(item.get("signal_id") or "") for item in alerts[:5]],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle-dir", default=str(BUNDLE_DIR_DEFAULT))
    args = parser.parse_args()

    bundle_dir = resolve_bundle_dir(args.bundle_dir)
    manifest = read_manifest(bundle_dir)
    documents = load_documents(bundle_dir, manifest)
    metrics, alerts = extract_findings(documents)

    structured = {
        "generated_at_utc": now_utc(),
        "bundle_dir": str(bundle_dir),
        "source_manifest": str(bundle_dir / "manifest.json"),
        "summary": build_summary(metrics, alerts, documents),
        "metrics": metrics,
        "alerts": alerts,
        "documents": [
            {
                key: value
                for key, value in document.items()
                if key != "_raw_text"
            }
            for document in documents
        ],
        "official_sources": [
            str(item.get("url") or "").strip()
            for item in manifest.get("downloaded_documents") or []
            if isinstance(item, dict) and str(item.get("url") or "").strip()
        ],
    }

    output_path = bundle_dir / "structured-evidence.json"
    output_path.write_text(json.dumps(structured, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(structured["summary"], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
