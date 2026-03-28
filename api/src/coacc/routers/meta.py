import json
import logging
import time
from dataclasses import replace
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from neo4j import AsyncSession

from coacc.config import settings
from coacc.dependencies import get_session
from coacc.models.dashboard import (
    PrioritizedBuyerResponse,
    PrioritizedBuyersResponse,
    PrioritizedCompaniesResponse,
    PrioritizedCompanyResponse,
    PrioritizedPeopleResponse,
    PrioritizedPersonResponse,
    PrioritizedTerritoriesResponse,
    PrioritizedTerritoryResponse,
    RiskAlertResponse,
    ValidationCaseResult,
    ValidationCasesResponse,
)
from coacc.services.neo4j_service import execute_query, execute_query_single
from coacc.services.public_guard import should_hide_person_entities
from coacc.services.source_registry import load_source_registry, source_registry_summary

router = APIRouter(prefix="/api/v1/meta", tags=["meta"])
logger = logging.getLogger(__name__)

_stats_cache: dict[str, Any] | None = None
_stats_cache_time: float = 0.0
_watchlist_cache: dict[int, tuple[float, PrioritizedPeopleResponse]] = {}
_company_watchlist_cache: dict[int, tuple[float, PrioritizedCompaniesResponse]] = {}
_buyer_watchlist_cache: dict[int, tuple[float, PrioritizedBuyersResponse]] = {}
_territory_watchlist_cache: dict[int, tuple[float, PrioritizedTerritoriesResponse]] = {}
_snapshot_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_SNAPSHOT_DIR = Path(__file__).resolve().parent.parent / "data" / "watchlists"
_MAX_WATCHLIST_LIMIT = 1000
_KNOWN_VALIDATION_CASES: tuple[dict[str, Any], ...] = (
    {
        "case_id": "fondecun_stalled_work",
        "title": "FONDECUN: stalled-work / elefante blanco style signal",
        "category": "elefante_blanco",
        "entity_type": "company",
        "entity_ref": "900258772",
        "expected_signals": ["budget_execution_discrepancy"],
        "metric_keys": [
            "contract_count",
            "contract_value",
            "execution_gap_contract_count",
            "execution_gap_invoice_total",
        ],
        "public_sources": [
            "https://www.integracionsocial.gov.co/index.php/noticias/116-otras-noticias/5795-contraloria-distrital-reconoce-a-integracion-social-los-avances-en-la-construccion-del-centro-dia-campo-verde",
            "https://www.elespectador.com/bogota/bosa-espera-recuperar-su-elefante-blanco/",
        ],
    },
    {
        "case_id": "fondecun_official_overlap",
        "title": "FONDECUN: official-supplier overlap",
        "category": "captura_contractual",
        "entity_type": "company",
        "entity_ref": "900258772",
        "expected_signals": ["public_official_supplier_overlap"],
        "metric_keys": [
            "contract_count",
            "contract_value",
            "official_officer_count",
            "official_role_count",
            "official_names",
        ],
        "public_sources": [
            "https://www.procuraduria.gov.co/Pages/procuraduria-alerta-ejecucion-3-billones-de-19-contrataderos.aspx",
            "https://fondecun.gov.co/download/815/2025/15530/resolucion-n-042-de-2025.pdf",
        ],
    },
    {
        "case_id": "egobus_sanctioned_supplier",
        "title": "EGOBUS SAS: sanctioned public operator",
        "category": "proveedor_sancionado",
        "entity_type": "company",
        "entity_ref": "900398793",
        "expected_signals": ["sanctioned_supplier_record"],
        "metric_keys": [
            "sanction_count",
            "contract_count",
            "contract_value",
        ],
        "public_sources": [
            "https://bogota.gov.co/mi-ciudad/movilidad/alcaldia-penalosa-empieza-pagarles-propietarios-egobus-y-coobus",
            "https://www.transmilenio.gov.co/files/6c7a31a0-0df6-4750-98f5-e1225e1b9583/0e64b507-6aa5-4be4-b90a-f1d3b050fe62/Informe%20de%20gestion%202016-2019.pdf",
        ],
    },
    {
        "case_id": "coobus_sanctioned_supplier",
        "title": "COOBUS SAS: sanctioned public operator",
        "category": "proveedor_sancionado",
        "entity_type": "company",
        "entity_ref": "900396145",
        "expected_signals": ["sanctioned_supplier_record"],
        "metric_keys": [
            "sanction_count",
            "contract_count",
            "contract_value",
        ],
        "public_sources": [
            "https://bogota.gov.co/mi-ciudad/movilidad/alcaldia-penalosa-empieza-pagarles-propietarios-egobus-y-coobus",
            "https://www.transmilenio.gov.co/files/6c7a31a0-0df6-4750-98f5-e1225e1b9583/0e64b507-6aa5-4be4-b90a-f1d3b050fe62/Informe%20de%20gestion%202016-2019.pdf",
        ],
    },
    {
        "case_id": "suministros_maybe_sanctioned_still_receiving",
        "title": "SUMINISTROS MAYBE S.A.S.: sanctioned supplier still receiving contracts",
        "category": "proveedor_sancionado",
        "entity_type": "company",
        "entity_ref": "800154801",
        "expected_signals": [
            "sanctioned_supplier_record",
            "sanctioned_still_receiving",
        ],
        "metric_keys": [
            "sanction_count",
            "contract_count",
            "contract_value",
            "sanctioned_still_receiving_contract_count",
            "sanctioned_still_receiving_total",
        ],
        "public_sources": [
            "https://sedeelectronica.sic.gov.co/sites/default/files/estados/032022/RELATOR%C3%8DA%20RESOLUCI%C3%93N%2012992%20DEL%2010%20DE%20MAYO%20DE%202019%20-%20SUMINISTROS.pdf",
            "https://www.pulzo.com/economia/carrusel-contratacion-estatal-PP476289",
        ],
    },
    {
        "case_id": "vivian_moreno_candidate_supplier",
        "title": "Vivian del Rosario Moreno Pérez: candidate-supplier overlap",
        "category": "riesgo_politico_contractual",
        "entity_type": "person",
        "entity_ref": "52184154",
        "expected_signals": ["candidate_supplier_overlap"],
        "metric_keys": [
            "candidacy_count",
            "donation_count",
            "supplier_contract_count",
            "supplier_contract_value",
            "linked_supplier_company_count",
        ],
        "public_sources": [
            "https://www.procuraduria.gov.co/Pages/cargos-siete-exediles-localidad-bogota-presuntas-irregularidades-conformacion-terna-alcalde-local.aspx",
        ],
    },
    {
        "case_id": "san_jose_education_control_capture",
        "title": "Fundación San José: control institucional con alias contractual",
        "category": "captura_educativa",
        "entity_type": "company",
        "entity_ref": "8605242195",
        "expected_signals": ["education_control_capture"],
        "metric_keys": [
            "education_director_count",
            "education_family_tie_count",
            "education_alias_count",
            "education_procurement_link_count",
            "education_procurement_total",
        ],
        "public_sources": [
            "https://www.mineducacion.gov.co/1780/w3-article-426421.html",
            "https://www.mineducacion.gov.co/1780/articles-426422_recurso_1.pdf",
            "https://caracol.com.co/2026/02/25/jennifer-pedraza-denuncia-que-directivos-de-la-san-jose-tienen-tentaculos-en-una-universidad-fachada/",
        ],
    },
    {
        "case_id": "alejandro_ospina_coll_bulletin_exposure",
        "title": "Alejandro Ospina Coll: boletin oficial con exposicion publica",
        "category": "supervision_disciplinaria",
        "entity_type": "person",
        "entity_ref": "10136043",
        "expected_signals": ["official_case_bulletin_exposure"],
        "metric_keys": [
            "official_case_bulletin_count",
            "person_sanction_count",
            "supplier_contract_count",
            "office_count",
        ],
        "public_sources": [
            "https://www.procuraduria.gov.co/Pages/procuraduria-confirmo-sancion-supervisor-contrato-omitio-vigilar-cumplimiento.aspx",
        ],
    },
    {
        "case_id": "federico_garcia_arbelaez_bulletin_exposure",
        "title": "Federico Garcia Arbelaez: boletin oficial con puente contractual",
        "category": "interventoria_pagos",
        "entity_type": "person",
        "entity_ref": "case_person_federico_garcia_arbelaez",
        "expected_signals": ["official_case_bulletin_exposure"],
        "metric_keys": [
            "official_case_bulletin_count",
            "linked_supplier_company_count",
            "supplier_contract_count",
            "supplier_contract_value",
        ],
        "public_sources": [
            "https://www.procuraduria.gov.co/Pages/cargos-a-contratista-del-sena-por-extralimitacion-en-funciones.aspx",
        ],
    },
    {
        "case_id": "jaime_jose_garces_garcia_bulletin_record",
        "title": "Jaime Jose Garces Garcia: boletin oficial de supervision",
        "category": "supervision_local",
        "entity_type": "person",
        "entity_ref": "case_person_jaime_jose_garces_garcia",
        "expected_signals": ["official_case_bulletin_record"],
        "metric_keys": [
            "official_case_bulletin_count",
        ],
        "public_sources": [
            "https://www.procuraduria.gov.co/Pages/cargos-funcionario-aguas-cesar-presuntas-irregularidades-supervision-contrato-interventoria.aspx",
        ],
    },
    {
        "case_id": "olmedo_lopez_ungrd_bulletin_record",
        "title": "Olmedo de Jesus Lopez Martinez: boletin oficial UNGRD",
        "category": "ungrd_direccionamiento",
        "entity_type": "person",
        "entity_ref": "case_person_olmedo_de_jesus_lopez_martinez",
        "expected_signals": ["official_case_bulletin_record"],
        "metric_keys": [
            "official_case_bulletin_count",
            "linked_supplier_company_count",
            "supplier_contract_count",
            "supplier_contract_value",
            "office_count",
        ],
        "public_sources": [
            "https://www.fiscalia.gov.co/colombia/noticias/nueva-imputacion-de-cargos-contra-exdirectivos-de-la-ungrd-olmedo-lopez-y-sneyder-pinilla-por-direccionamiento-irregular-de-la-contratacion-en-la-entidad/",
        ],
    },
    {
        "case_id": "sneyder_pinilla_ungrd_bulletin_record",
        "title": "Sneyder Augusto Pinilla Alvarez: boletin oficial UNGRD",
        "category": "ungrd_direccionamiento",
        "entity_type": "person",
        "entity_ref": "case_person_sneyder_augusto_pinilla_alvarez",
        "expected_signals": ["official_case_bulletin_record"],
        "metric_keys": [
            "official_case_bulletin_count",
            "linked_supplier_company_count",
            "supplier_contract_count",
            "supplier_contract_value",
            "office_count",
        ],
        "public_sources": [
            "https://www.fiscalia.gov.co/colombia/noticias/nueva-imputacion-de-cargos-contra-exdirectivos-de-la-ungrd-olmedo-lopez-y-sneyder-pinilla-por-direccionamiento-irregular-de-la-contratacion-en-la-entidad/",
        ],
    },
    {
        "case_id": "carlos_ramon_gonzalez_ungrd_bulletin_record",
        "title": "Carlos Ramon Gonzalez Merchan: boletin oficial UNGRD",
        "category": "ungrd_dadivas",
        "entity_type": "person",
        "entity_ref": "case_person_carlos_ramon_gonzalez_merchan",
        "expected_signals": ["official_case_bulletin_record"],
        "metric_keys": [
            "official_case_bulletin_count",
            "office_count",
        ],
        "public_sources": [
            "https://www.fiscalia.gov.co/colombia/lucha-contra-corrupcion/acusado-exdirector-del-departamento-administrativo-de-presidencia-carlos-ramon-gonzalez-merchan-por-presuntamente-direccionar-dadivas-en-favor-de-congresistas-con-recursos-de-la-ungrd",
        ],
    },
    {
        "case_id": "luis_carlos_barreto_ungrd_bulletin_record",
        "title": "Luis Carlos Barreto Gantiva: boletin oficial UNGRD",
        "category": "ungrd_preacuerdo",
        "entity_type": "person",
        "entity_ref": "case_person_luis_carlos_barreto_gantiva",
        "expected_signals": ["official_case_bulletin_record"],
        "metric_keys": [
            "official_case_bulletin_count",
            "linked_supplier_company_count",
            "supplier_contract_count",
            "supplier_contract_value",
        ],
        "public_sources": [
            "https://www.fiscalia.gov.co/colombia/noticias/exdirector-de-conocimiento-de-la-ungrd-luis-carlos-barreto-gantiva-sera-condenado-mediante-preacuerdo-por-direccionamiento-de-contratos-en-la-entidad/",
        ],
    },
    {
        "case_id": "sandra_ortiz_ungrd_bulletin_record",
        "title": "Sandra Liliana Ortiz Nova: boletin oficial UNGRD",
        "category": "ungrd_dadivas",
        "entity_type": "person",
        "entity_ref": "case_person_sandra_liliana_ortiz_nova",
        "expected_signals": ["official_case_bulletin_record"],
        "metric_keys": [
            "official_case_bulletin_count",
            "office_count",
            "linked_supplier_company_count",
            "supplier_contract_count",
            "supplier_contract_value",
        ],
        "public_sources": [
            "https://www.fiscalia.gov.co/colombia/noticias/acusada-exconsejera-presidencial-para-las-regiones-sandra-ortiz-por-presuntamente-trasladar-dadivas-relacionadas-con-actos-de-corrupcion-en-la-ungrd/",
        ],
    },
    {
        "case_id": "maria_alejandra_benavides_ungrd_bulletin_record",
        "title": "Maria Alejandra Benavides Soto: boletin oficial UNGRD",
        "category": "ungrd_intervencion_ministerio",
        "entity_type": "person",
        "entity_ref": "case_person_maria_alejandra_benavides_soto",
        "expected_signals": ["official_case_bulletin_record"],
        "metric_keys": [
            "official_case_bulletin_count",
            "office_count",
        ],
        "public_sources": [
            "https://www.fiscalia.gov.co/colombia/noticias/imputada-exasesora-del-ministerio-de-hacienda-por-su-presunta-intervencion-en-el-direccionamiento-de-contratos-en-la-ungrd-en-favor-de-congresistas/",
        ],
    },
)


def _compact_float(value: float | int | None) -> str:
    amount = float(value or 0.0)
    if amount >= 1_000_000_000:
        return f"COP {amount / 1_000_000_000:.1f} mil millones"
    if amount >= 1_000_000:
        return f"COP {amount / 1_000_000:.1f} millones"
    if amount >= 1_000:
        return f"COP {amount / 1_000:.1f} mil"
    return f"COP {amount:.0f}"


def _share_text(value: float | int | None) -> str:
    share = max(0.0, min(float(value or 0.0), 1.0))
    return f"{share * 100:.1f}%"


def _clean_refs(*refs: str | None) -> list[str]:
    return [ref for ref in refs if ref]


def _risk_alert(
    *,
    alert_type: str,
    finding_class: str,
    severity_score: int,
    confidence_tier: str,
    reason_text: str,
    evidence_refs: list[str],
    source_list: list[str],
    what_is_unproven: str,
    next_step: str,
) -> RiskAlertResponse:
    return RiskAlertResponse(
        alert_type=alert_type,
        finding_class=finding_class,
        severity_score=severity_score,
        confidence_tier=confidence_tier,
        reason_text=reason_text,
        evidence_refs=evidence_refs,
        source_list=source_list,
        human_review_needed=True,
        what_is_unproven=what_is_unproven,
        next_step=next_step,
    )


def _secop_discrepancy_phrase(
    execution_gap_contract_count: int,
    commitment_gap_contract_count: int,
) -> str:
    if execution_gap_contract_count > 0 and commitment_gap_contract_count > 0:
        return "brechas entre compromiso, facturación y avance de ejecución"
    if commitment_gap_contract_count > 0:
        return "brechas entre compromiso presupuestal y facturación"
    return "brechas entre facturación y avance de ejecución"


def _secop_discrepancy_sources(
    execution_gap_contract_count: int,
    commitment_gap_contract_count: int,
) -> list[str]:
    sources = ["SECOP II facturas", "SECOP / SECOP II contratos"]
    if execution_gap_contract_count > 0:
        sources.insert(1, "SECOP II ejecución contratos")
    if commitment_gap_contract_count > 0:
        sources.insert(1, "SECOP II compromisos")
    return sources


def _load_watchlist_snapshot(name: str) -> list[dict[str, Any]] | None:
    cached = _snapshot_cache.get(name)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < 300:
        return cached[1]

    path = _SNAPSHOT_DIR / f"{name}.json"
    if not path.exists():
        return None

    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError:
        return None

    rows = payload.get(name)
    if not isinstance(rows, list):
        return None

    normalized = [row for row in rows if isinstance(row, dict)]
    _snapshot_cache[name] = (now, normalized)
    return normalized


def _extract_metrics(
    record: dict[str, Any] | None,
    keys: list[str],
) -> dict[str, str | float | int | bool | list[str] | None]:
    if record is None:
        return {}
    metrics: dict[str, str | float | int | bool | list[str] | None] = {}
    for key in keys:
        value = record.get(key)
        metrics[key] = [str(item) for item in value] if isinstance(value, list) else value
    return metrics


def _validation_status(
    expected_signals: list[str],
    observed_signals: list[str],
) -> tuple[str, list[str]]:
    normalized_observed = set(observed_signals)
    if "official_case_bulletin_exposure" in normalized_observed:
        normalized_observed.add("official_case_bulletin_record")
    matched_signals = [signal for signal in expected_signals if signal in normalized_observed]
    if not observed_signals:
        return "missing", matched_signals
    if len(matched_signals) == len(expected_signals):
        return "matched", matched_signals
    if matched_signals:
        return "partial", matched_signals
    return "missing", matched_signals


def _build_person_alerts(record: dict[str, Any]) -> list[RiskAlertResponse]:
    alerts: list[RiskAlertResponse] = []
    document_id = record.get("document_id")
    case_person_id = record.get("case_person_id")

    office_count = int(record.get("office_count") or 0)
    sensitive_office_count = int(record.get("sensitive_office_count") or 0)
    supplier_contract_count = int(record.get("supplier_contract_count") or 0)
    linked_supplier_company_count = int(record.get("linked_supplier_company_count") or 0)
    person_sanction_count = int(record.get("person_sanction_count") or 0)
    disciplinary_sanction_count = int(record.get("disciplinary_sanction_count") or 0)
    fiscal_responsibility_count = int(record.get("fiscal_responsibility_count") or 0)
    donor_vendor_loop_count = int(record.get("donor_vendor_loop_count") or 0)
    donation_count = int(record.get("donation_count") or 0)
    candidacy_count = int(record.get("candidacy_count") or 0)
    payment_supervision_count = int(record.get("payment_supervision_count") or 0)
    payment_supervision_company_count = int(
        record.get("payment_supervision_company_count") or 0
    )
    payment_supervision_risk_contract_count = int(
        record.get("payment_supervision_risk_contract_count") or 0
    )
    payment_supervision_discrepancy_contract_count = int(
        record.get("payment_supervision_discrepancy_contract_count") or 0
    )
    payment_supervision_suspension_contract_count = int(
        record.get("payment_supervision_suspension_contract_count") or 0
    )
    payment_supervision_pending_contract_count = int(
        record.get("payment_supervision_pending_contract_count") or 0
    )
    payment_supervision_archive_contract_count = int(
        record.get("payment_supervision_archive_contract_count") or 0
    )
    archive_document_total = int(record.get("archive_document_total") or 0)
    archive_supervision_document_total = int(
        record.get("archive_supervision_document_total") or 0
    )
    archive_payment_document_total = int(record.get("archive_payment_document_total") or 0)
    archive_assignment_document_total = int(
        record.get("archive_assignment_document_total") or 0
    )
    official_case_bulletin_count = int(record.get("official_case_bulletin_count") or 0)
    official_case_bulletin_titles = [
        str(title)
        for title in (record.get("official_case_bulletin_titles") or [])
        if str(title).strip()
    ]
    disclosure_reference_count = int(record.get("disclosure_reference_count") or 0)
    conflict_disclosure_count = int(record.get("conflict_disclosure_count") or 0)
    corporate_activity_disclosure_count = int(
        record.get("corporate_activity_disclosure_count") or 0
    )

    if official_case_bulletin_count > 0:
        exposure_parts: list[str] = []
        if person_sanction_count > 0:
            exposure_parts.append(f"{person_sanction_count} sancion(es)")
        if supplier_contract_count > 0:
            exposure_parts.append(
                f"{supplier_contract_count} contrato(s) como proveedora o contratista"
            )
        if office_count > 0:
            exposure_parts.append(f"{office_count} cargo(s) publicos")
        if payment_supervision_count > 0:
            exposure_parts.append(f"{payment_supervision_count} supervision(es) de pago")

        bulletin_title = official_case_bulletin_titles[0] if official_case_bulletin_titles else None
        matched_exposure = bool(exposure_parts)
        alerts.append(
            _risk_alert(
                alert_type=(
                    "official_case_bulletin_exposure"
                    if matched_exposure
                    else "official_case_bulletin_record"
                ),
                finding_class="official_case_record",
                severity_score=min(
                    94,
                    34
                    + official_case_bulletin_count * 10
                    + person_sanction_count * 3
                    + supplier_contract_count * 3
                    + office_count * 4
                    + payment_supervision_risk_contract_count * 5,
                ),
                confidence_tier="A",
                reason_text=(
                    (
                        "La persona aparece en "
                        f"{official_case_bulletin_count} boletin(es) oficial(es)"
                        + (f" ('{bulletin_title}')" if bulletin_title else "")
                        + " y ademas mantiene "
                        + ", ".join(exposure_parts)
                        + " dentro del grafo vivo."
                    )
                    if matched_exposure
                    else (
                        "La persona aparece en "
                        f"{official_case_bulletin_count} boletin(es) oficial(es)"
                        + (f" ('{bulletin_title}')" if bulletin_title else "")
                        + ", aunque el grafo aun no cierra mas exposiciones estructuradas."
                    )
                ),
                evidence_refs=_clean_refs(
                    f"documento:{document_id}" if document_id else None,
                    f"caso:{case_person_id}" if case_person_id else None,
                    f"boletines:{official_case_bulletin_count}",
                    f"contratos:{supplier_contract_count}" if supplier_contract_count else None,
                    f"cargos:{office_count}" if office_count else None,
                    f"sanciones:{person_sanction_count}" if person_sanction_count else None,
                ),
                source_list=["Procuraduria / Fiscalia / Contraloria / MEN - boletines oficiales"],
                what_is_unproven=(
                    "El boletin oficial documenta una actuacion o expediente, pero no sustituye"
                    " la revision completa del caso, sus anexos y el estado final del proceso."
                ),
                next_step=(
                    "Abra el boletin y sus anexos, confirme cronologia, entidad y rol concreto,"
                    " y contraste si las otras exposiciones del grafo ocurrieron antes, durante"
                    " o despues del expediente."
                ),
            )
        )

    if office_count > 0 and supplier_contract_count > 0:
        is_sensitive_overlap = sensitive_office_count > 0
        reason_text = (
            (
                "La misma persona aparece en cargos públicos sensibles"
                " y también como proveedora o directiva de proveedor"
            )
            if is_sensitive_overlap
            else "El mismo documento aparece en cargos públicos y también como proveedor"
        )
        reason_text += f" con {supplier_contract_count} contrato(s) registrados."
        if is_sensitive_overlap:
            reason_text += (
                f" {sensitive_office_count} cargo(s) están marcados como sensibles."
            )
        alerts.append(
            _risk_alert(
                alert_type=(
                    "sensitive_public_official_supplier_overlap"
                    if is_sensitive_overlap
                    else "public_official_supplier_overlap"
                ),
                finding_class="incompatibility",
                severity_score=min(
                    96,
                    55
                    + office_count * 5
                    + supplier_contract_count * 3
                    + sensitive_office_count * 6,
                ),
                confidence_tier="A",
                reason_text=reason_text,
                evidence_refs=_clean_refs(
                    f"documento:{document_id}" if document_id else None,
                    f"cargos:{office_count}",
                    (
                        f"cargos_sensibles:{sensitive_office_count}"
                        if is_sensitive_overlap
                        else None
                    ),
                    f"contratos:{supplier_contract_count}",
                ),
                source_list=(
                    ["SIGEP II", "Puestos Sensibles SIGEP", "SECOP / SECOP II"]
                    if is_sensitive_overlap
                    else ["SIGEP II", "SECOP / SECOP II"]
                ),
                what_is_unproven=(
                    "No prueba por sí solo un conflicto ilegal; faltan fechas, funciones"
                    " exactas y revisión de la entidad contratante."
                ),
                next_step=(
                    "Verifique si el cargo público y los contratos coinciden en tiempo,"
                    " entidad o función decisoria."
                ),
            )
        )

    if person_sanction_count > 0 and (
        office_count > 0
        or supplier_contract_count > 0
        or donation_count > 0
        or candidacy_count > 0
        or payment_supervision_count > 0
    ):
        sanction_parts: list[str] = []
        if disciplinary_sanction_count > 0:
            sanction_parts.append(
                f"{disciplinary_sanction_count} sanción(es) disciplinaria(s)"
            )
        if fiscal_responsibility_count > 0:
            sanction_parts.append(
                f"{fiscal_responsibility_count} sanción(es) de responsabilidad fiscal"
            )
        if not sanction_parts:
            sanction_parts.append(f"{person_sanction_count} sanción(es) registradas")

        exposure_parts: list[str] = []
        if office_count > 0:
            exposure_parts.append(f"{office_count} cargo(s) públicos")
        if supplier_contract_count > 0:
            exposure_parts.append(f"{supplier_contract_count} contrato(s) como proveedora o directiva")
        if payment_supervision_count > 0:
            exposure_parts.append(f"{payment_supervision_count} contrato(s) en supervisión de pago")
        if donation_count > 0:
            exposure_parts.append(f"{donation_count} donación(es) electorales")
        if candidacy_count > 0:
            exposure_parts.append(f"{candidacy_count} candidatura(s)")

        source_list = ["SIRI / Responsabilidad Fiscal / PACO"]
        if office_count > 0:
            source_list.append("SIGEP II")
        if supplier_contract_count > 0 or payment_supervision_count > 0:
            source_list.append("SECOP / SECOP II")
        if payment_supervision_count > 0 and "SECOP II - Plan de pagos" not in source_list:
            source_list.append("SECOP II - Plan de pagos")
        if donation_count > 0 or candidacy_count > 0:
            source_list.append("Cuentas Claras / elecciones")

        alerts.append(
            _risk_alert(
                alert_type="sanctioned_person_exposure_stack",
                finding_class="prior_risk",
                severity_score=min(
                    95,
                    44
                    + person_sanction_count * 4
                    + supplier_contract_count * 4
                    + office_count * 4
                    + payment_supervision_risk_contract_count * 5
                    + donation_count * 2
                    + candidacy_count * 4,
                ),
                confidence_tier="A",
                reason_text=(
                    "La persona registra "
                    + ", ".join(sanction_parts)
                    + " en fuentes oficiales de control y además aparece con "
                    + ", ".join(exposure_parts)
                    + "."
                ),
                evidence_refs=_clean_refs(
                    f"documento:{document_id}" if document_id else None,
                    f"sanciones_persona:{person_sanction_count}",
                    (
                        f"responsabilidad_fiscal:{fiscal_responsibility_count}"
                        if fiscal_responsibility_count
                        else None
                    ),
                    f"contratos:{supplier_contract_count}" if supplier_contract_count else None,
                    f"cargos:{office_count}" if office_count else None,
                ),
                source_list=source_list,
                what_is_unproven=(
                    "El antecedente sancionatorio no demuestra por sí solo una nueva falta;"
                    " aún hay que revisar fechas, estado de la sanción y la relación exacta"
                    " con la contratación o el cargo público."
                ),
                next_step=(
                    "Abra el expediente sancionatorio, contraste su cronología con contratos,"
                    " actividad electoral o cargo público, y verifique si hubo continuidad"
                    " de exposición pública pese al antecedente."
                ),
            )
        )

    if payment_supervision_count > 0 and payment_supervision_risk_contract_count > 0 and (
        office_count > 0 or donation_count > 0 or candidacy_count > 0
    ):
        stack_parts: list[str] = []
        if payment_supervision_discrepancy_contract_count > 0:
            stack_parts.append("brechas de ejecución o compromiso")
        if payment_supervision_suspension_contract_count > 0:
            stack_parts.append("suspensiones")
        if payment_supervision_pending_contract_count > 0:
            stack_parts.append("pagos pendientes")
        if not stack_parts:
            stack_parts.append("riesgos de ejecución o pago")

        source_list = [
            "SECOP II - Plan de pagos",
            "SECOP / SECOP II contratos",
        ]
        if payment_supervision_discrepancy_contract_count > 0:
            for source in ["SECOP II facturas", "SECOP II ejecución contratos"]:
                if source not in source_list:
                    source_list.append(source)
        if payment_supervision_suspension_contract_count > 0:
            source_list.append("SECOP II - Suspensiones de Contratos")
        if office_count > 0:
            source_list.append("SIGEP II")
        if donation_count > 0 or candidacy_count > 0:
            source_list.append("Cuentas Claras / elecciones")
        if payment_supervision_archive_contract_count > 0:
            source_list.append("SECOP II - Archivos Descarga Desde 2025")

        alerts.append(
            _risk_alert(
                alert_type="payment_supervision_risk_stack",
                finding_class="oversight_risk",
                severity_score=min(
                    95,
                    46
                    + payment_supervision_risk_contract_count * 8
                    + office_count * 5
                    + donation_count * 2
                    + candidacy_count * 4,
                ),
                confidence_tier="A",
                reason_text=(
                    "La persona figura como supervisor(a) de pago en "
                    f"{payment_supervision_count} contrato(s) sobre "
                    f"{payment_supervision_company_count} proveedor(es), y "
                    f"{payment_supervision_risk_contract_count} muestran "
                    + ", ".join(stack_parts)
                    + (
                        f". Además, el expediente público ya aporta "
                        f"{payment_supervision_archive_contract_count} contrato(s) con "
                        f"{archive_document_total} soporte(s), incluyendo "
                        f"{archive_supervision_document_total} de supervisión, "
                        f"{archive_payment_document_total} de pago y "
                        f"{archive_assignment_document_total} de designación o delegación."
                        if payment_supervision_archive_contract_count > 0
                        else "."
                    )
                ),
                evidence_refs=_clean_refs(
                    f"documento:{document_id}" if document_id else None,
                    f"contratos_supervisados:{payment_supervision_count}",
                    f"contratos_riesgo:{payment_supervision_risk_contract_count}",
                    (
                        f"pagos_pendientes:{payment_supervision_pending_contract_count}"
                        if payment_supervision_pending_contract_count
                        else None
                    ),
                    (
                        f"suspensiones:{payment_supervision_suspension_contract_count}"
                        if payment_supervision_suspension_contract_count
                        else None
                    ),
                    (
                        f"archivos_supervisados:{payment_supervision_archive_contract_count}"
                        if payment_supervision_archive_contract_count
                        else None
                    ),
                    (
                        f"soportes_supervision:{archive_supervision_document_total}"
                        if archive_supervision_document_total
                        else None
                    ),
                    (
                        f"soportes_pago:{archive_payment_document_total}"
                        if archive_payment_document_total
                        else None
                    ),
                ),
                source_list=source_list,
                what_is_unproven=(
                    "La supervisión o aprobación de pago no prueba por sí sola una falta;"
                    " todavía hay que revisar actas, alcance funcional y soportes del pago."
                ),
                next_step=(
                    "Revise actas de supervisión, informes de interventoría, soportes de pago"
                    " y la cronología entre cargo público, supervisión y ejecución del contrato."
                ),
            )
        )

    if candidacy_count > 0 and supplier_contract_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="candidate_supplier_overlap",
                finding_class="incompatibility",
                severity_score=min(
                    90,
                    42 + candidacy_count * 6 + supplier_contract_count * 4 + donation_count * 2,
                ),
                confidence_tier="A",
                reason_text=(
                    "La misma persona aparece en candidaturas electorales y también como"
                    " proveedora o directiva de proveedor con contratación pública."
                ),
                evidence_refs=_clean_refs(
                    f"documento:{document_id}" if document_id else None,
                    f"candidaturas:{candidacy_count}",
                    f"contratos:{supplier_contract_count}",
                ),
                source_list=["Cuentas Claras / elecciones", "SECOP / SECOP II"],
                what_is_unproven=(
                    "La coincidencia no demuestra una conducta ilegal por sí sola; faltan"
                    " fechas, entidad contratante y revisión de inhabilidades aplicables."
                ),
                next_step=(
                    "Revise cronología de candidaturas, contratos y eventuales vínculos"
                    " con la entidad compradora."
                ),
            )
        )

    if donation_count > 0 and supplier_contract_count > 0 and donor_vendor_loop_count == 0:
        alerts.append(
            _risk_alert(
                alert_type="donor_supplier_overlap",
                finding_class="concentration",
                severity_score=min(84, 38 + donation_count * 4 + supplier_contract_count * 4),
                confidence_tier="A",
                reason_text=(
                    "La misma persona aparece como donante electoral y también como"
                    " proveedora o directiva de proveedor con contratación pública."
                ),
                evidence_refs=_clean_refs(
                    f"documento:{document_id}" if document_id else None,
                    f"donaciones:{donation_count}",
                    f"contratos:{supplier_contract_count}",
                ),
                source_list=["Cuentas Claras", "SECOP / SECOP II"],
                what_is_unproven=(
                    "La coocurrencia no prueba quid pro quo; aún requiere contrastar"
                    " campañas receptoras, cronología y relación con los compradores."
                ),
                next_step=(
                    "Cruce aportes, campañas y adjudicaciones para verificar si la"
                    " contratación ocurrió después de la financiación política."
                ),
            )
        )

    if donor_vendor_loop_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="donor_official_vendor_loop",
                finding_class="incompatibility",
                severity_score=min(94, 50 + donor_vendor_loop_count * 6 + donation_count),
                confidence_tier="A",
                reason_text=(
                    "La misma persona aparece en donaciones electorales y contratación pública,"
                    " formando un circuito donante-funcionario-proveedor."
                ),
                evidence_refs=_clean_refs(
                    f"documento:{document_id}" if document_id else None,
                    f"donaciones:{donation_count}",
                    f"contratos:{supplier_contract_count}",
                ),
                source_list=["Cuentas Claras", "SIGEP II", "SECOP / SECOP II"],
                what_is_unproven=(
                    "No demuestra intercambio indebido; aún hay que contrastar campañas,"
                    " adjudicaciones y relación con el comprador público."
                ),
                next_step=(
                    "Compare fechas de donación, candidaturas y adjudicaciones para ver"
                    " si la contratación ocurrió después del aporte."
                ),
            )
        )

    if linked_supplier_company_count >= 2 and supplier_contract_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="shared_officer_supplier_network",
                finding_class="concentration",
                severity_score=min(
                    90,
                    40 + linked_supplier_company_count * 8 + min(supplier_contract_count, 6) * 3,
                ),
                confidence_tier="A",
                reason_text=(
                    "La misma persona figura como directiva o representante en varias"
                    " empresas proveedoras que también reciben contratos públicos."
                ),
                evidence_refs=_clean_refs(
                    f"documento:{document_id}" if document_id else None,
                    f"empresas_relacionadas:{linked_supplier_company_count}",
                    f"contratos:{supplier_contract_count}",
                ),
                source_list=["RUES / registros societarios", "SECOP / SECOP II"],
                what_is_unproven=(
                    "El control compartido no demuestra colusión por sí solo; falta revisar"
                    " si hubo coordinación indebida, ofertas espejo o reparto de compradores."
                ),
                next_step=(
                    "Compare compradores, calendarios, objetos y competidores para ver si"
                    " la red rota contratos entre empresas vinculadas."
                ),
            )
        )

    if (
        disclosure_reference_count > 0
        or conflict_disclosure_count > 0
        or corporate_activity_disclosure_count > 0
    ):
        alerts.append(
            _risk_alert(
                alert_type="disclosure_risk_stack",
                finding_class=(
                    "textual_mention" if disclosure_reference_count > 0 else "prior_risk"
                ),
                severity_score=min(
                    88,
                    35
                    + disclosure_reference_count * 3
                    + conflict_disclosure_count * 2
                    + corporate_activity_disclosure_count * 6,
                ),
                confidence_tier="C" if disclosure_reference_count > 0 else "B",
                reason_text=(
                    "Las declaraciones oficiales muestran intereses privados o referencias"
                    f" textuales relevantes: {disclosure_reference_count} mención(es),"
                    f" {conflict_disclosure_count} declaración(es) de conflicto y"
                    f" {corporate_activity_disclosure_count} señal(es) de actividad corporativa."
                ),
                evidence_refs=_clean_refs(
                    f"documento:{document_id}" if document_id else None,
                    (
                        f"menciones:{disclosure_reference_count}"
                        if disclosure_reference_count
                        else None
                    ),
                    (
                        f"intereses_privados:{corporate_activity_disclosure_count}"
                        if corporate_activity_disclosure_count
                        else None
                    ),
                ),
                source_list=["Ley 2013 / Integridad Pública", "SECOP / SECOP II"],
                what_is_unproven=(
                    "Las menciones textuales y banderas declaradas no identifican por sí solas"
                    " un beneficiario final ni una conducta ilegal."
                ),
                next_step=(
                    "Revise la declaración original y contraste empresas o procesos mencionados"
                    " con RUES/RUP y contratos asociados."
                ),
            )
        )

    return sorted(alerts, key=lambda alert: alert.severity_score, reverse=True)[:3]


async def _validate_company_case(
    session: AsyncSession,
    case: dict[str, Any],
) -> ValidationCaseResult:
    record = await execute_query_single(
        session,
        "meta_validation_company_case",
        {
            "document_id": case["entity_ref"],
            "pattern_split_threshold_value": settings.pattern_split_threshold_value,
            "pattern_split_min_average_value": settings.pattern_split_min_average_value,
            "pattern_split_min_total_value": settings.pattern_split_min_total_value,
            "pattern_split_min_count": settings.pattern_split_min_count,
        },
    )
    if record is None:
        return ValidationCaseResult(
            case_id=case["case_id"],
            title=case["title"],
            category=case["category"],
            entity_type=case["entity_type"],
            entity_ref=case["entity_ref"],
            status="not_found",
            matched=False,
            expected_signals=case["expected_signals"],
            observed_signals=[],
            matched_signals=[],
            summary="La entidad no existe en el grafo vivo cargado.",
            metrics={},
            public_sources=case["public_sources"],
        )

    normalized = dict(record)
    alerts = _build_company_alerts(normalized)
    observed_signals = [alert.alert_type for alert in alerts]
    status, matched_signals = _validation_status(case["expected_signals"], observed_signals)
    summary = (
        next(
            (
                alert.reason_text
                for alert in alerts
                if alert.alert_type in matched_signals
            ),
            None,
        )
        or (alerts[0].reason_text if alerts else "No se detectaron señales priorizadas.")
    )
    return ValidationCaseResult(
        case_id=case["case_id"],
        title=case["title"],
        category=case["category"],
        entity_type=case["entity_type"],
        entity_ref=case["entity_ref"],
        entity_id=normalized.get("entity_id"),
        entity_name=normalized.get("name"),
        status=status,
        matched=status == "matched",
        expected_signals=case["expected_signals"],
        observed_signals=observed_signals,
        matched_signals=matched_signals,
        summary=summary,
        metrics=_extract_metrics(normalized, case["metric_keys"]),
        public_sources=case["public_sources"],
    )


async def _validate_person_case(
    session: AsyncSession,
    case: dict[str, Any],
) -> ValidationCaseResult:
    record = await execute_query_single(
        session,
        "meta_validation_person_case",
        {"person_ref": case["entity_ref"]},
    )
    if record is None:
        return ValidationCaseResult(
            case_id=case["case_id"],
            title=case["title"],
            category=case["category"],
            entity_type=case["entity_type"],
            entity_ref=case["entity_ref"],
            status="not_found",
            matched=False,
            expected_signals=case["expected_signals"],
            observed_signals=[],
            matched_signals=[],
            summary="La persona no existe en el grafo vivo cargado.",
            metrics={},
            public_sources=case["public_sources"],
        )

    normalized = dict(record)
    alerts = _build_person_alerts(normalized)
    observed_signals = [alert.alert_type for alert in alerts]
    status, matched_signals = _validation_status(case["expected_signals"], observed_signals)
    summary = (
        next(
            (
                alert.reason_text
                for alert in alerts
                if alert.alert_type in matched_signals
            ),
            None,
        )
        or (alerts[0].reason_text if alerts else "No se detectaron señales priorizadas.")
    )
    return ValidationCaseResult(
        case_id=case["case_id"],
        title=case["title"],
        category=case["category"],
        entity_type=case["entity_type"],
        entity_ref=case["entity_ref"],
        entity_id=normalized.get("entity_id"),
        entity_name=normalized.get("name"),
        status=status,
        matched=status == "matched",
        expected_signals=case["expected_signals"],
        observed_signals=observed_signals,
        matched_signals=matched_signals,
        summary=summary,
        metrics=_extract_metrics(normalized, case["metric_keys"]),
        public_sources=case["public_sources"],
    )


def _build_company_alerts(record: dict[str, Any]) -> list[RiskAlertResponse]:
    alerts: list[RiskAlertResponse] = []
    document_id = record.get("document_id")
    company_type = str(record.get("company_type") or "")

    sanction_count = int(record.get("sanction_count") or 0)
    official_officer_count = int(record.get("official_officer_count") or 0)
    sensitive_officer_count = int(record.get("sensitive_officer_count") or 0)
    sensitive_role_count = int(record.get("sensitive_role_count") or 0)
    funding_overlap_event_count = int(record.get("funding_overlap_event_count") or 0)
    execution_gap_contract_count = int(record.get("execution_gap_contract_count") or 0)
    commitment_gap_contract_count = int(record.get("commitment_gap_contract_count") or 0)
    interadmin_agreement_count = int(record.get("interadmin_agreement_count") or 0)
    interadmin_risk_contract_count = int(record.get("interadmin_risk_contract_count") or 0)
    suspension_contract_count = int(record.get("suspension_contract_count") or 0)
    suspension_event_count = int(record.get("suspension_event_count") or 0)
    sanctioned_still_receiving_contract_count = int(
        record.get("sanctioned_still_receiving_contract_count") or 0
    )
    split_contract_group_count = int(record.get("split_contract_group_count") or 0)
    archive_contract_count = int(record.get("archive_contract_count") or 0)
    archive_document_total = int(record.get("archive_document_total") or 0)
    archive_supervision_contract_count = int(
        record.get("archive_supervision_contract_count") or 0
    )
    archive_supervision_document_total = int(
        record.get("archive_supervision_document_total") or 0
    )
    archive_payment_contract_count = int(record.get("archive_payment_contract_count") or 0)
    archive_payment_document_total = int(record.get("archive_payment_document_total") or 0)
    archive_assignment_contract_count = int(
        record.get("archive_assignment_contract_count") or 0
    )
    archive_assignment_document_total = int(
        record.get("archive_assignment_document_total") or 0
    )
    education_director_count = int(record.get("education_director_count") or 0)
    education_family_tie_count = int(record.get("education_family_tie_count") or 0)
    education_alias_count = int(record.get("education_alias_count") or 0)
    education_procurement_link_count = int(record.get("education_procurement_link_count") or 0)
    fiscal_finding_count = int(record.get("fiscal_finding_count") or 0)
    discrepancy_phrase = _secop_discrepancy_phrase(
        execution_gap_contract_count,
        commitment_gap_contract_count,
    )
    discrepancy_sources = _secop_discrepancy_sources(
        execution_gap_contract_count,
        commitment_gap_contract_count,
    )
    capacity_ratio = max(
        float(record.get("capacity_mismatch_revenue_ratio") or 0.0),
        float(record.get("capacity_mismatch_asset_ratio") or 0.0),
    )

    if sanction_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="sanctioned_supplier_record",
                finding_class="prior_risk",
                severity_score=min(95, 45 + sanction_count * 5),
                confidence_tier="A",
                reason_text=(
                    f"La empresa tiene {sanction_count} antecedente(s) sancionatorios"
                    " y mantiene exposición en contratación pública."
                ),
                evidence_refs=_clean_refs(
                    f"nit:{document_id}" if document_id else None,
                    f"sanciones:{sanction_count}",
                    f"contratos:{record.get('contract_count')}",
                ),
                source_list=["SIRI / Responsabilidad Fiscal / PACO", "SECOP sanciones", "SECOP / SECOP II"],
                what_is_unproven=(
                    "El antecedente sancionatorio no prueba una irregularidad en cada contrato"
                    " actual; requiere revisar causa, vigencia y comprador."
                ),
                next_step=(
                    "Abra el expediente sancionatorio y contraste fechas, entidad emisora"
                    " y contratos vigentes."
                ),
            )
        )

    if sanctioned_still_receiving_contract_count > 0:
        source_list = ["SIRI / Responsabilidad Fiscal / PACO", "SECOP / SECOP II"]
        if archive_contract_count > 0:
            source_list.append("SECOP II - Archivos Descarga Desde 2025")
        alerts.append(
            _risk_alert(
                alert_type="sanctioned_still_receiving",
                finding_class="prior_risk",
                severity_score=min(96, 56 + sanctioned_still_receiving_contract_count * 8),
                confidence_tier="A",
                reason_text=(
                    "La empresa registra al menos un contrato con fecha dentro"
                    " de una ventana pública de sanción vigente o no cerrada."
                    + (
                        f" Además, {archive_contract_count} contrato(s) ya tienen "
                        f"{archive_document_total} soporte(s) documentales visibles,"
                        f" incluyendo {archive_supervision_document_total} de supervisión"
                        f" y {archive_payment_document_total} de pago."
                        if archive_contract_count > 0
                        else ""
                    )
                ),
                evidence_refs=_clean_refs(
                    f"nit:{document_id}" if document_id else None,
                    f"contratos_en_ventana_sancion:{sanctioned_still_receiving_contract_count}",
                    (
                        f"valor_en_ventana:{_compact_float(record.get('sanctioned_still_receiving_total'))}"
                        if float(record.get("sanctioned_still_receiving_total") or 0.0) > 0.0
                        else None
                    ),
                    f"contratos_archivo:{archive_contract_count}" if archive_contract_count else None,
                    (
                        f"soportes_supervision:{archive_supervision_document_total}"
                        if archive_supervision_document_total
                        else None
                    ),
                    (
                        f"soportes_pago:{archive_payment_document_total}"
                        if archive_payment_document_total
                        else None
                    ),
                ),
                source_list=source_list,
                what_is_unproven=(
                    "La coincidencia de fechas no demuestra por sí sola ilegalidad;"
                    " hay que revisar si la sanción cubría exactamente ese contrato"
                    " y si hubo suspensión, cesión o excepción aplicable."
                ),
                next_step=(
                    "Contraste la resolución sancionatoria con las fechas de adjudicación,"
                    " ejecución y pago del contrato para verificar si hubo continuidad"
                    " contractual durante la vigencia de la restricción."
                ),
            )
        )

    if fiscal_finding_count > 0:
        source_list = ["Hallazgos Fiscales"]
        if int(record.get("contract_count") or 0) > 0:
            source_list.append("SECOP / SECOP II")
        alerts.append(
            _risk_alert(
                alert_type="fiscal_finding_record",
                finding_class="audit_signal",
                severity_score=min(92, 42 + fiscal_finding_count * 6),
                confidence_tier="A",
                reason_text=(
                    "La entidad registra hallazgo(s) fiscal(es) comunicados en"
                    f" fuentes oficiales de control ({fiscal_finding_count})."
                ),
                evidence_refs=_clean_refs(
                    f"nit:{document_id}" if document_id else None,
                    f"hallazgos:{fiscal_finding_count}",
                    (
                        f"cuantia_hallazgos:{_compact_float(record.get('fiscal_finding_total'))}"
                        if float(record.get("fiscal_finding_total") or 0.0) > 0.0
                        else None
                    ),
                ),
                source_list=source_list,
                what_is_unproven=(
                    "Un hallazgo fiscal es una señal oficial de control, no una condena"
                    " final ni una prueba automática de responsabilidad individual."
                ),
                next_step=(
                    "Abra el radicado del hallazgo, revise hechos, estado del trámite y"
                    " si las vigencias observadas coinciden con la actividad contractual"
                    " o presupuestal de la entidad."
                ),
            )
        )

    if sensitive_officer_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="sensitive_public_official_supplier_overlap",
                finding_class="incompatibility",
                severity_score=min(
                    95,
                    56 + sensitive_officer_count * 9 + sensitive_role_count * 4,
                ),
                confidence_tier="A",
                reason_text=(
                    "La empresa comparte dirección o control con persona(s) que también"
                    " aparecen en cargos públicos sensibles o de mayor exposición al riesgo."
                ),
                evidence_refs=_clean_refs(
                    f"nit:{document_id}" if document_id else None,
                    f"oficiales_sensibles:{sensitive_officer_count}",
                    f"roles_sensibles:{sensitive_role_count}",
                    f"oficiales_relacionados:{official_officer_count}" if official_officer_count else None,
                ),
                source_list=[
                    "SIGEP II",
                    "Puestos Sensibles SIGEP",
                    "RUES / registros societarios",
                    "SECOP / SECOP II",
                ],
                what_is_unproven=(
                    "La coincidencia con un cargo sensible no prueba intervención indebida"
                    " sin revisar funciones, fechas y alcance del vínculo societario."
                ),
                next_step=(
                    "Verifique si la persona tenía funciones de ordenación del gasto,"
                    " supervisión, control o influencia sobre la entidad contratante."
                ),
            )
        )
    if official_officer_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="public_official_supplier_overlap",
                finding_class="incompatibility",
                severity_score=min(92, 50 + official_officer_count * 8),
                confidence_tier="A",
                reason_text=(
                    "La empresa comparte dirección o control con persona(s) que también"
                    " aparecen en registros activos de cargo o salario público."
                ),
                evidence_refs=_clean_refs(
                    f"nit:{document_id}" if document_id else None,
                    f"oficiales_relacionados:{official_officer_count}",
                    f"roles_publicos:{record.get('official_role_count')}",
                ),
                source_list=["SIGEP II", "RUES / registros societarios", "SECOP / SECOP II"],
                what_is_unproven=(
                    "La coincidencia no prueba participación indebida en la decisión"
                    " contractual sin revisar funciones y periodos."
                ),
                next_step=(
                    "Verifique si la persona tenía rol decisorio o de supervisión"
                    " sobre la entidad compradora."
                ),
            )
        )

    if company_type == "INSTITUCION_EDUCACION_SUPERIOR" and (
        education_alias_count > 0
        or education_family_tie_count > 0
        or education_procurement_link_count > 0
    ):
        education_stack_reasons: list[str] = []
        if education_alias_count > 0:
            education_stack_reasons.append("alias institucional en otras bases")
        if education_procurement_link_count > 0:
            education_stack_reasons.append("vínculo con convenios o contratación pública")
        if education_family_tie_count > 0:
            education_stack_reasons.append("controladores con posible parentesco")

        alerts.append(
            _risk_alert(
                alert_type="education_control_capture",
                finding_class="channeling",
                severity_score=min(
                    88,
                    38
                    + education_director_count * 6
                    + education_alias_count * 9
                    + education_family_tie_count * 7
                    + education_procurement_link_count * 4,
                ),
                confidence_tier="A",
                reason_text=(
                    "La institución concentra administración y además aparece con "
                    + ", ".join(education_stack_reasons)
                    + "."
                ),
                evidence_refs=_clean_refs(
                    f"nit:{document_id}" if document_id else None,
                    f"directivos:{education_director_count}" if education_director_count else None,
                    f"alias_detectados:{education_alias_count}" if education_alias_count else None,
                    (
                        f"convenios_relacionados:{education_procurement_link_count}"
                        if education_procurement_link_count
                        else None
                    ),
                ),
                source_list=[
                    "MEN instituciones de educación superior",
                    "MEN directivos de IES",
                    "SECOP II - Convenios Interadministrativos",
                ],
                what_is_unproven=(
                    "La red institucional no prueba por sí sola expedición irregular"
                    " de títulos ni responsabilidad penal; requiere revisar resoluciones,"
                    " expedientes y actos internos."
                ),
                next_step=(
                    "Revise la resolución del MEN, compare alias contractuales,"
                    " representantes legales y convenios asociados para consolidar"
                    " el mapa de control institucional."
                ),
            )
        )

    if interadmin_agreement_count > 0 and (
        interadmin_risk_contract_count > 0 or official_officer_count > 0 or sanction_count > 0
    ):
        stack_reasons: list[str] = []
        if official_officer_count > 0:
            stack_reasons.append("directivos en cargo público")
        if sanction_count > 0:
            stack_reasons.append("antecedentes sancionatorios")
        if interadmin_risk_contract_count > 0:
            stack_reasons.append("contratos con brechas de ejecución, compromiso o suspensión")

        source_list: list[str] = [
            "SECOP II - Convenios Interadministrativos",
            "SECOP / SECOP II contratos",
        ]
        if official_officer_count > 0:
            source_list.append("SIGEP II")
        if sanction_count > 0:
            source_list.append("PACO / SIRI")
        if interadmin_risk_contract_count > 0:
            for source in discrepancy_sources:
                if source not in source_list:
                    source_list.append(source)
        if (
            suspension_contract_count > 0
            and "SECOP II - Suspensiones de Contratos" not in source_list
        ):
            source_list.append("SECOP II - Suspensiones de Contratos")
        if archive_contract_count > 0:
            source_list.append("SECOP II - Archivos Descarga Desde 2025")

        alerts.append(
            _risk_alert(
                alert_type="interadministrative_channel_stacking",
                finding_class="channeling",
                severity_score=min(
                    92,
                    38
                    + interadmin_agreement_count
                    + interadmin_risk_contract_count * 5
                    + official_officer_count * 7
                    + sanction_count * 4,
                ),
                confidence_tier="A",
                reason_text=(
                    "La empresa aparece como contraparte en convenios interadministrativos"
                    " y también como contratista ordinaria; el apilamiento coincide con "
                    + ", ".join(stack_reasons)
                    + (
                        f". Además, el archivo SECOP visible ya cubre "
                        f"{archive_contract_count} contrato(s) con "
                        f"{archive_document_total} soporte(s), incluyendo "
                        f"{archive_supervision_document_total} de supervisión y "
                        f"{archive_assignment_document_total} de designación o delegación."
                        if archive_contract_count > 0
                        else "."
                    )
                ),
                evidence_refs=_clean_refs(
                    f"nit:{document_id}" if document_id else None,
                    f"convenios_interadmin:{interadmin_agreement_count}",
                    (
                        f"contratos_riesgo:{interadmin_risk_contract_count}"
                        if interadmin_risk_contract_count
                        else None
                    ),
                    f"valor_interadmin:{_compact_float(record.get('interadmin_total'))}",
                    f"contratos_archivo:{archive_contract_count}" if archive_contract_count else None,
                ),
                source_list=source_list,
                what_is_unproven=(
                    "La coexistencia de convenio interadministrativo y contratación ordinaria"
                    " no prueba desvío por sí sola; requiere revisar objeto, subcontratación"
                    " y trazabilidad de la ejecución."
                ),
                next_step=(
                    "Revise el convenio interadministrativo, su subcontratación, los contratos"
                    " ordinarios vinculados y quién controló la supervisión y los pagos."
                ),
            )
        )

    if capacity_ratio >= 2.0:
        alerts.append(
            _risk_alert(
                alert_type="company_capacity_mismatch",
                finding_class="discrepancy",
                severity_score=min(90, 42 + int(capacity_ratio * 6)),
                confidence_tier="A",
                reason_text=(
                    "La exposición contractual supera de forma material la escala financiera"
                    f" reportada por la empresa ({capacity_ratio:.1f}x)."
                ),
                evidence_refs=_clean_refs(
                    f"nit:{document_id}" if document_id else None,
                    f"ratio_capacidad:{capacity_ratio:.1f}x",
                    f"valor_contratos:{_compact_float(record.get('contract_value'))}",
                ),
                source_list=["Supersociedades / SIIS", "SECOP / SECOP II"],
                what_is_unproven=(
                    "Un descalce de escala no prueba simulación; puede haber consorcios,"
                    " subcontratación o reportes financieros rezagados."
                ),
                next_step=(
                    "Revise capacidad técnica, consorcios, experiencia habilitante"
                    " y soportes financieros del proceso."
                ),
            )
        )

    if execution_gap_contract_count > 0 or commitment_gap_contract_count > 0:
        source_list = list(discrepancy_sources)
        if archive_contract_count > 0:
            source_list.append("SECOP II - Archivos Descarga Desde 2025")
        alerts.append(
            _risk_alert(
                alert_type="budget_execution_discrepancy",
                finding_class="discrepancy",
                severity_score=min(
                    91,
                    40 + execution_gap_contract_count * 6 + commitment_gap_contract_count * 6,
                ),
                confidence_tier="A",
                reason_text=(
                    f"Los contratos asociados muestran {discrepancy_phrase}."
                    + (
                        f" Ya hay {archive_contract_count} contrato(s) con "
                        f"{archive_document_total} soporte(s) públicos para contrastar el expediente,"
                        f" incluyendo {archive_supervision_contract_count} con documentos de supervisión"
                        f" y {archive_payment_contract_count} con documentos de pago."
                        if archive_contract_count > 0
                        else ""
                    )
                ),
                evidence_refs=_clean_refs(
                    f"sin_ejecucion:{execution_gap_contract_count}"
                    if execution_gap_contract_count
                    else None,
                    (
                        f"sobre_compromiso:{commitment_gap_contract_count}"
                        if commitment_gap_contract_count
                        else None
                    ),
                    f"nit:{document_id}" if document_id else None,
                    f"contratos_archivo:{archive_contract_count}" if archive_contract_count else None,
                ),
                source_list=source_list,
                what_is_unproven=(
                    "La brecha puede obedecer a rezagos de cargue o hitos parciales;"
                    " requiere revisión documental del contrato."
                ),
                next_step=(
                    "Pida actas, informes de interventoría y soportes de factura"
                    " para confirmar si el bien o servicio fue ejecutado."
                ),
            )
        )

    if suspension_contract_count > 0:
        source_list = [
            "SECOP II - Suspensiones de Contratos",
            "SECOP / SECOP II contratos",
        ]
        if archive_contract_count > 0:
            source_list.append("SECOP II - Archivos Descarga Desde 2025")
        alerts.append(
            _risk_alert(
                alert_type="contract_suspension_stacking",
                finding_class="discrepancy",
                severity_score=min(
                    89,
                    38 + suspension_contract_count * 4 + suspension_event_count * 6,
                ),
                confidence_tier="A",
                reason_text=(
                    "La empresa aparece en contratos con suspensiones reiteradas,"
                    " una señal sensible de ejecución trabada o manipulación postadjudicación."
                    + (
                        f" Además, el archivo público ya cubre {archive_contract_count} contrato(s)"
                        f" con {archive_document_total} soporte(s)."
                        if archive_contract_count > 0
                        else ""
                    )
                ),
                evidence_refs=_clean_refs(
                    f"contratos_suspendidos:{suspension_contract_count}",
                    f"suspensiones:{suspension_event_count}" if suspension_event_count else None,
                    f"nit:{document_id}" if document_id else None,
                    f"contratos_archivo:{archive_contract_count}" if archive_contract_count else None,
                ),
                source_list=source_list,
                what_is_unproven=(
                    "La suspensión no prueba desvío por sí sola; puede responder a causas"
                    " técnicas, climáticas o jurídicas legítimas."
                ),
                next_step=(
                    "Revise el acto de suspensión, las actas de reinicio, la interventoría"
                    " y si hubo facturación o adiciones durante la pausa."
                ),
            )
        )

    if split_contract_group_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="split_contracts_below_threshold",
                finding_class="channeling",
                severity_score=min(84, 34 + split_contract_group_count * 8),
                confidence_tier="B",
                reason_text=(
                    "La empresa aparece en paquetes repetidos de contratos"
                    " con valores medios bajos y valor total agregado suficiente"
                    " para revisar posible fraccionamiento."
                ),
                evidence_refs=_clean_refs(
                    f"nit:{document_id}" if document_id else None,
                    f"grupos_fraccionados:{split_contract_group_count}",
                    (
                        f"valor_fraccionado:{_compact_float(record.get('split_contract_total'))}"
                        if float(record.get("split_contract_total") or 0.0) > 0.0
                        else None
                    ),
                ),
                source_list=["SECOP / SECOP II"],
                what_is_unproven=(
                    "La recurrencia debajo de un umbral no prueba fraccionamiento"
                    " sin revisar objeto contractual, necesidad real y planeación."
                ),
                next_step=(
                    "Compare objeto, fechas, comprador y estudios previos para verificar"
                    " si varios contratos debieron tramitarse como un solo proceso competitivo."
                ),
            )
        )

    if funding_overlap_event_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="funding_spike_then_awards",
                finding_class="concentration",
                severity_score=min(80, 28 + funding_overlap_event_count * 3),
                confidence_tier="B",
                reason_text=(
                    "La empresa aparece tanto en contratación como en ejecución de recursos"
                    " públicos upstream, lo que la vuelve prioritaria para revisión."
                ),
                evidence_refs=_clean_refs(
                    f"eventos_upstream:{funding_overlap_event_count}",
                    f"monto_upstream:{_compact_float(record.get('funding_overlap_total'))}",
                    f"nit:{document_id}" if document_id else None,
                ),
                source_list=["SGR / PTE", "SECOP / SECOP II"],
                what_is_unproven=(
                    "La coincidencia aún no demuestra un flujo causal completo"
                    " entre financiación y adjudicación."
                ),
                next_step=(
                    "Cruce fechas de asignación, compromisos y adjudicación para verificar"
                    " si hubo concentración posterior al ingreso de recursos."
                ),
            )
        )

    return sorted(alerts, key=lambda alert: alert.severity_score, reverse=True)[:5]


def _build_buyer_alerts(record: dict[str, Any]) -> list[RiskAlertResponse]:
    alerts: list[RiskAlertResponse] = []
    buyer_ref = record.get("buyer_document_id") or record.get("buyer_name")
    top_supplier_share = float(record.get("top_supplier_share") or 0.0)
    discrepancy_contract_count = int(record.get("discrepancy_contract_count") or 0)
    execution_gap_contract_count = int(record.get("execution_gap_contract_count") or 0)
    commitment_gap_contract_count = int(record.get("commitment_gap_contract_count") or 0)
    sanctioned_supplier_contract_count = int(record.get("sanctioned_supplier_contract_count") or 0)
    official_overlap_supplier_count = int(record.get("official_overlap_supplier_count") or 0)
    capacity_mismatch_supplier_count = int(record.get("capacity_mismatch_supplier_count") or 0)
    low_competition_contract_count = int(record.get("low_competition_contract_count") or 0)
    direct_invitation_contract_count = int(record.get("direct_invitation_contract_count") or 0)
    fiscal_finding_count = int(record.get("fiscal_finding_count") or 0)
    discrepancy_phrase = _secop_discrepancy_phrase(
        execution_gap_contract_count,
        commitment_gap_contract_count,
    )
    discrepancy_sources = _secop_discrepancy_sources(
        execution_gap_contract_count,
        commitment_gap_contract_count,
    )

    if top_supplier_share >= 0.35:
        alerts.append(
            _risk_alert(
                alert_type="buyer_supplier_concentration",
                finding_class="concentration",
                severity_score=min(94, 35 + int(top_supplier_share * 100)),
                confidence_tier="A",
                reason_text=(
                    "Una sola empresa concentra una porción relevante del gasto contractual"
                    f" del comprador ({_share_text(top_supplier_share)})."
                ),
                evidence_refs=_clean_refs(
                    f"comprador:{buyer_ref}" if buyer_ref else None,
                    f"proveedor_top:{record.get('top_supplier_name')}",
                    f"participacion:{_share_text(top_supplier_share)}",
                ),
                source_list=["SECOP / SECOP II"],
                what_is_unproven=(
                    "La concentración no prueba direccionamiento por sí sola;"
                    " puede responder a especialización real o pocos oferentes."
                ),
                next_step=(
                    "Revise objetos repetidos, cronología de procesos y si los mismos"
                    " competidores pierden de forma sistemática."
                ),
            )
        )

    if low_competition_contract_count > 0 or direct_invitation_contract_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="low_competition_cluster",
                finding_class="concentration",
                severity_score=min(
                    88,
                    30 + low_competition_contract_count * 4 + direct_invitation_contract_count * 5,
                ),
                confidence_tier="B",
                reason_text=(
                    "El comprador acumula procesos con baja competencia o invitación directa,"
                    " una combinación sensible a direccionamiento."
                ),
                evidence_refs=_clean_refs(
                    f"comprador:{buyer_ref}" if buyer_ref else None,
                    (
                        f"baja_competencia:{low_competition_contract_count}"
                        if low_competition_contract_count
                        else None
                    ),
                    (
                        f"invitacion_directa:{direct_invitation_contract_count}"
                        if direct_invitation_contract_count
                        else None
                    ),
                ),
                source_list=["SECOP / SECOP II"],
                what_is_unproven=(
                    "Los procesos de baja competencia no demuestran fraude sin revisar"
                    " pliegos, mercado relevante y causales del procedimiento."
                ),
                next_step=(
                    "Revise pliegos, oferentes habilitados y cambios de cronograma"
                    " en los procesos del mismo comprador."
                ),
            )
        )

    if sanctioned_supplier_contract_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="sanctioned_supplier_exposure",
                finding_class="prior_risk",
                severity_score=min(90, 40 + sanctioned_supplier_contract_count * 4),
                confidence_tier="A",
                reason_text=(
                    "El comprador adjudicó contratos a proveedores con antecedentes"
                    " sancionatorios en fuentes públicas."
                ),
                evidence_refs=_clean_refs(
                    f"comprador:{buyer_ref}" if buyer_ref else None,
                    f"contratos_sancionados:{sanctioned_supplier_contract_count}",
                    f"valor:{_compact_float(record.get('sanctioned_supplier_value'))}",
                ),
                source_list=["SIRI / Responsabilidad Fiscal / PACO", "SECOP sanciones", "SECOP / SECOP II"],
                what_is_unproven=(
                    "La exposición no prueba una irregularidad del comprador sin verificar"
                    " la vigencia de la sanción y el contexto del proceso."
                ),
                next_step=(
                    "Revise la vigencia de cada sanción y la trazabilidad del proceso de compra."
                ),
            )
        )

    if fiscal_finding_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="buyer_fiscal_findings_exposure",
                finding_class="audit_signal",
                severity_score=min(90, 38 + fiscal_finding_count * 6),
                confidence_tier="A",
                reason_text=(
                    "El comprador coincide con una entidad que registra hallazgos"
                    f" fiscales oficiales ({fiscal_finding_count})."
                ),
                evidence_refs=_clean_refs(
                    f"comprador:{buyer_ref}" if buyer_ref else None,
                    f"hallazgos:{fiscal_finding_count}",
                    (
                        f"cuantia_hallazgos:{_compact_float(record.get('fiscal_finding_total'))}"
                        if float(record.get("fiscal_finding_total") or 0.0) > 0.0
                        else None
                    ),
                ),
                source_list=["Hallazgos Fiscales", "SECOP / SECOP II"],
                what_is_unproven=(
                    "El hallazgo fiscal es una alerta oficial sobre la entidad auditada;"
                    " no demuestra por sí solo que cada contrato del comprador sea irregular."
                ),
                next_step=(
                    "Revise el radicado del hallazgo y contraste si los contratos del"
                    " comprador coinciden en vigencia, objeto o dependencia auditada."
                ),
            )
        )

    if official_overlap_supplier_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="official_overlap_supplier_exposure",
                finding_class="incompatibility",
                severity_score=min(89, 38 + official_overlap_supplier_count * 7),
                confidence_tier="A",
                reason_text=(
                    "El comprador contrató proveedores ligados a personas que también"
                    " figuran en registros activos de cargo o salario público."
                ),
                evidence_refs=_clean_refs(
                    f"comprador:{buyer_ref}" if buyer_ref else None,
                    f"proveedores_relacionados:{official_overlap_supplier_count}",
                    f"contratos:{record.get('official_overlap_contract_count')}",
                ),
                source_list=["SIGEP II", "SECOP / SECOP II"],
                what_is_unproven=(
                    "La coincidencia no prueba participación indebida del funcionario"
                    " sin revisar sus competencias y fechas."
                ),
                next_step=(
                    "Identifique a los funcionarios vinculados y compare sus periodos"
                    " con la adjudicación y la supervisión del contrato."
                ),
            )
        )

    if capacity_mismatch_supplier_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="capacity_mismatch_supplier_exposure",
                finding_class="discrepancy",
                severity_score=min(84, 34 + capacity_mismatch_supplier_count * 6),
                confidence_tier="B",
                reason_text=(
                    "El comprador concentra contratos en proveedores cuya escala financiera"
                    " reportada luce pequeña frente al valor contratado."
                ),
                evidence_refs=_clean_refs(
                    f"comprador:{buyer_ref}" if buyer_ref else None,
                    f"proveedores_descalzados:{capacity_mismatch_supplier_count}",
                ),
                source_list=["Supersociedades / SIIS", "SECOP / SECOP II"],
                what_is_unproven=(
                    "El descalce puede tener explicaciones legítimas como consorcios,"
                    " subcontratación o rezago contable."
                ),
                next_step=(
                    "Revise capacidad financiera habilitante, experiencia específica"
                    " y estructura societaria del proveedor."
                ),
            )
        )

    if discrepancy_contract_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="buyer_budget_execution_discrepancy",
                finding_class="discrepancy",
                severity_score=min(90, 36 + discrepancy_contract_count * 6),
                confidence_tier="A",
                reason_text=f"Los contratos del comprador muestran {discrepancy_phrase}.",
                evidence_refs=_clean_refs(
                    f"comprador:{buyer_ref}" if buyer_ref else None,
                    f"contratos_con_brecha:{discrepancy_contract_count}",
                    (
                        f"sin_ejecucion:{execution_gap_contract_count}"
                        if execution_gap_contract_count
                        else None
                    ),
                    (
                        f"sobre_compromiso:{commitment_gap_contract_count}"
                        if commitment_gap_contract_count
                        else None
                    ),
                    f"valor_brecha:{_compact_float(record.get('discrepancy_value'))}",
                ),
                source_list=discrepancy_sources,
                what_is_unproven=(
                    "La brecha puede venir de rezagos de reporte; todavía necesita"
                    " validación contractual y presupuestal."
                ),
                next_step=(
                    "Solicite actas de ejecución, soportes de pago y reportes presupuestales"
                    " del comprador."
                ),
            )
        )

    return sorted(alerts, key=lambda alert: alert.severity_score, reverse=True)[:3]


def _build_territory_alerts(record: dict[str, Any]) -> list[RiskAlertResponse]:
    alerts: list[RiskAlertResponse] = []
    territory_ref = record.get("territory_name")
    top_supplier_share = float(record.get("top_supplier_share") or 0.0)
    low_competition_contract_count = int(record.get("low_competition_contract_count") or 0)
    direct_invitation_contract_count = int(record.get("direct_invitation_contract_count") or 0)
    sanctioned_supplier_contract_count = int(record.get("sanctioned_supplier_contract_count") or 0)
    official_overlap_contract_count = int(record.get("official_overlap_contract_count") or 0)
    capacity_mismatch_supplier_count = int(record.get("capacity_mismatch_supplier_count") or 0)
    discrepancy_contract_count = int(record.get("discrepancy_contract_count") or 0)
    execution_gap_contract_count = int(record.get("execution_gap_contract_count") or 0)
    commitment_gap_contract_count = int(record.get("commitment_gap_contract_count") or 0)
    uses_project_snapshot = (
        int(record.get("supplier_count") or 0) == 0
        and int(record.get("buyer_count") or 0) > 0
    )
    discrepancy_phrase = _secop_discrepancy_phrase(
        execution_gap_contract_count,
        commitment_gap_contract_count,
    )
    discrepancy_sources = _secop_discrepancy_sources(
        execution_gap_contract_count,
        commitment_gap_contract_count,
    )

    if top_supplier_share >= 0.35:
        alerts.append(
            _risk_alert(
                alert_type="territory_supplier_concentration",
                finding_class="concentration",
                severity_score=min(92, 35 + int(top_supplier_share * 100)),
                confidence_tier="A",
                reason_text=(
                    "Una sola empresa concentra una porción alta de la contratación"
                    f" observada en {territory_ref} ({_share_text(top_supplier_share)})."
                ),
                evidence_refs=_clean_refs(
                    f"territorio:{territory_ref}" if territory_ref else None,
                    f"proveedor_top:{record.get('top_supplier_name')}",
                    f"participacion:{_share_text(top_supplier_share)}",
                ),
                source_list=["SECOP / SECOP II"],
                what_is_unproven=(
                    "La concentración territorial no prueba favoritismo sin revisar"
                    " mercado local, objeto contractual y competencia efectiva."
                ),
                next_step=(
                    "Compare objetos, compradores y calendarios para ver si la misma empresa"
                    " domina compras recurrentes del territorio."
                ),
            )
        )

    if low_competition_contract_count > 0 or direct_invitation_contract_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="territory_low_competition_cluster",
                finding_class="concentration",
                severity_score=min(
                    86,
                    30 + low_competition_contract_count * 4 + direct_invitation_contract_count * 5,
                ),
                confidence_tier="B",
                reason_text=(
                    "El territorio concentra contratos con baja competencia o invitación"
                    " directa, una señal útil para priorizar revisión."
                ),
                evidence_refs=_clean_refs(
                    f"territorio:{territory_ref}" if territory_ref else None,
                    (
                        f"baja_competencia:{low_competition_contract_count}"
                        if low_competition_contract_count
                        else None
                    ),
                    (
                        f"invitacion_directa:{direct_invitation_contract_count}"
                        if direct_invitation_contract_count
                        else None
                    ),
                ),
                source_list=["SECOP / SECOP II"],
                what_is_unproven=(
                    "No demuestra colusión sin revisar oferentes, requisitos habilitantes"
                    " y condiciones de mercado."
                ),
                next_step=(
                    "Revise los procesos con un solo oferente y compare si los mismos"
                    " proveedores reaparecen en el territorio."
                ),
            )
        )

    if sanctioned_supplier_contract_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="territory_sanctioned_supplier_exposure",
                finding_class="prior_risk",
                severity_score=min(88, 38 + sanctioned_supplier_contract_count * 4),
                confidence_tier="A",
                reason_text=(
                    f"En {territory_ref} aparecen contratos con proveedores sancionados"
                    " o con antecedentes disciplinarios públicos."
                ),
                evidence_refs=_clean_refs(
                    f"territorio:{territory_ref}" if territory_ref else None,
                    f"contratos_sancionados:{sanctioned_supplier_contract_count}",
                    f"valor:{_compact_float(record.get('sanctioned_supplier_value'))}",
                ),
                source_list=["SIRI / Responsabilidad Fiscal / PACO", "SECOP sanciones", "SECOP / SECOP II"],
                what_is_unproven=(
                    "El antecedente no prueba irregularidad en cada contrato del territorio;"
                    " requiere revisar vigencia y contexto."
                ),
                next_step=(
                    "Cruce comprador, proveedor y fecha de sanción para ver si hubo"
                    " reiteración después del antecedente."
                ),
            )
        )

    if official_overlap_contract_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="territory_official_overlap_exposure",
                finding_class="incompatibility",
                severity_score=min(86, 34 + official_overlap_contract_count * 4),
                confidence_tier="B",
                reason_text=(
                    "El territorio registra contratos con proveedores vinculados"
                    " a personas en cargo o salario público."
                ),
                evidence_refs=_clean_refs(
                    f"territorio:{territory_ref}" if territory_ref else None,
                    f"contratos_relacionados:{official_overlap_contract_count}",
                ),
                source_list=["SIGEP II", "SECOP / SECOP II"],
                what_is_unproven=(
                    "El vínculo requiere validar rol público, fechas y si la persona"
                    " tuvo capacidad de influir en la contratación local."
                ),
                next_step=(
                    "Identifique a los funcionarios y verifique si pertenecen al mismo"
                    " municipio, departamento o entidad compradora."
                ),
            )
        )

    if capacity_mismatch_supplier_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="territory_capacity_mismatch_exposure",
                finding_class="discrepancy",
                severity_score=min(82, 30 + capacity_mismatch_supplier_count * 6),
                confidence_tier="B",
                reason_text=(
                    f"En {territory_ref} aparecen proveedores cuya escala financiera"
                    " luce pequeña frente a los contratos adjudicados."
                ),
                evidence_refs=_clean_refs(
                    f"territorio:{territory_ref}" if territory_ref else None,
                    f"proveedores_descalzados:{capacity_mismatch_supplier_count}",
                ),
                source_list=["Supersociedades / SIIS", "SECOP / SECOP II"],
                what_is_unproven=(
                    "El descalce puede ser legítimo si existe consorcio, respaldo financiero"
                    " o actividad subcontratada."
                ),
                next_step=(
                    "Revise si los proveedores operan como consorcio, unión temporal"
                    " o si el contrato exige capacidad técnica superior."
                ),
            )
        )

    if discrepancy_contract_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="territory_budget_execution_discrepancy",
                finding_class="discrepancy",
                severity_score=min(88, 34 + discrepancy_contract_count * 5),
                confidence_tier="A",
                reason_text=(
                    (
                        f"En {territory_ref} hay proyectos públicos con brechas entre"
                        " avance financiero y avance físico."
                    )
                    if uses_project_snapshot
                    else (
                        f"En {territory_ref} hay contratos con señales de {discrepancy_phrase}."
                    )
                ),
                evidence_refs=_clean_refs(
                    f"territorio:{territory_ref}" if territory_ref else None,
                    f"contratos_con_brecha:{discrepancy_contract_count}",
                    (
                        f"sin_ejecucion:{execution_gap_contract_count}"
                        if execution_gap_contract_count
                        else None
                    ),
                    (
                        f"sobre_compromiso:{commitment_gap_contract_count}"
                        if commitment_gap_contract_count
                        else None
                    ),
                    f"valor_brecha:{_compact_float(record.get('discrepancy_value'))}",
                ),
                source_list=(
                    ["MapaInversiones", "PTE / SGR"]
                    if uses_project_snapshot
                    else discrepancy_sources
                ),
                what_is_unproven=(
                    "La brecha aún puede explicarse por rezagos de reporte y necesita"
                    " validación documental."
                ),
                next_step=(
                    "Priorice los registros con mayor valor y pida soportes de avance"
                    " físico, financiero y actas de seguimiento."
                ),
            )
        )

    return sorted(alerts, key=lambda alert: alert.severity_score, reverse=True)[:3]


async def _load_registry_with_runtime_status(
    session: AsyncSession,
) -> list[Any]:
    entries = load_source_registry()
    status_records = await execute_query(session, "meta_source_load_status", {})
    status_by_source = {
        (record.get("source_id") or ""): (record.get("status") or "")
        for record in status_records
    }

    updated_entries = []
    for entry in entries:
        runtime_status = status_by_source.get(entry.id, "").strip().lower()
        if runtime_status == "loaded":
            updated_entries.append(
                replace(
                    entry,
                    status="loaded",
                    load_state="loaded",
                )
            )
        else:
            updated_entries.append(entry)
    return updated_entries


@router.get("/health")
async def neo4j_health(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, str]:
    record = await execute_query_single(session, "health_check", {})
    if record and record["ok"] == 1:
        return {"neo4j": "connected"}
    return {"neo4j": "error"}


@router.get("/stats")
async def database_stats(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, Any]:
    global _stats_cache, _stats_cache_time  # noqa: PLW0603

    if _stats_cache is not None and (time.monotonic() - _stats_cache_time) < 300:
        return _stats_cache

    record = await execute_query_single(session, "meta_stats", {})
    source_entries = await _load_registry_with_runtime_status(session)
    source_summary = source_registry_summary(source_entries)

    result = {
        "total_nodes": record["total_nodes"] if record else 0,
        "total_relationships": record["total_relationships"] if record else 0,
        "person_count": (
            0 if should_hide_person_entities() else (record["person_count"] if record else 0)
        ),
        "company_count": record["company_count"] if record else 0,
        "health_count": record["health_count"] if record else 0,
        "finance_count": record["finance_count"] if record else 0,
        "contract_count": record["contract_count"] if record else 0,
        "sanction_count": record["sanction_count"] if record else 0,
        "election_count": record["election_count"] if record else 0,
        "amendment_count": record["amendment_count"] if record else 0,
        "education_count": record["education_count"] if record else 0,
        "bid_count": record["bid_count"] if record else 0,
        "source_document_count": record.get("source_document_count", 0) if record else 0,
        "ingestion_run_count": record.get("ingestion_run_count", 0) if record else 0,
        "data_sources": source_summary["universe_v1_sources"],
        "implemented_sources": source_summary["implemented_sources"],
        "loaded_sources": source_summary["loaded_sources"],
        "healthy_sources": source_summary["healthy_sources"],
        "stale_sources": source_summary["stale_sources"],
        "blocked_external_sources": source_summary["blocked_external_sources"],
        "quality_fail_sources": source_summary["quality_fail_sources"],
        "promoted_sources": source_summary["promoted_sources"],
        "enrichment_only_sources": source_summary["enrichment_only_sources"],
        "quarantined_sources": source_summary["quarantined_sources"],
        "discovered_uningested_sources": source_summary["discovered_uningested_sources"],
    }

    _stats_cache = result
    _stats_cache_time = time.monotonic()
    return result


@router.get("/sources")
async def list_sources(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, list[dict[str, Any]]]:
    source_entries = await _load_registry_with_runtime_status(session)
    sources = [entry.to_public_dict() for entry in source_entries if entry.in_universe_v1]
    return {"sources": sources}


@router.get("/validation/known-cases", response_model=ValidationCasesResponse)
async def validate_known_cases(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> ValidationCasesResponse:
    cases: list[ValidationCaseResult] = []
    for case in _KNOWN_VALIDATION_CASES:
        if case["entity_type"] == "company":
            result = await _validate_company_case(session, case)
        else:
            result = await _validate_person_case(session, case)
        cases.append(result)

    matched = sum(1 for case in cases if case.matched)
    return ValidationCasesResponse(cases=cases, total=len(cases), matched=matched)


@router.get("/watchlist/people", response_model=PrioritizedPeopleResponse)
async def prioritized_people_watchlist(
    session: Annotated[AsyncSession, Depends(get_session)],
    limit: int = 12,
) -> PrioritizedPeopleResponse:
    if should_hide_person_entities():
        return PrioritizedPeopleResponse(people=[], total=0)

    safe_limit = max(1, min(limit, _MAX_WATCHLIST_LIMIT))
    cached = _watchlist_cache.get(safe_limit)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < 300:
        return cached[1]

    snapshot_rows = _load_watchlist_snapshot("people")
    if snapshot_rows is not None and len(snapshot_rows) >= safe_limit:
        records = snapshot_rows[:safe_limit]
    else:
        try:
            records = await execute_query(
                session,
                "meta_prioritized_people",
                {
                    "limit": safe_limit,
                    "pattern_min_discrepancy_ratio": settings.pattern_min_discrepancy_ratio,
                },
                timeout=8,
            )
        except Exception:
            if snapshot_rows is None:
                raise
            logger.exception("Falling back to people watchlist snapshot")
            records = snapshot_rows[:safe_limit]
    response = PrioritizedPeopleResponse(
        people=[
            PrioritizedPersonResponse(
                entity_id=record["entity_id"],
                name=record["name"],
                document_id=record.get("document_id"),
                case_person_id=record.get("case_person_id"),
                suspicion_score=record["suspicion_score"],
                signal_types=record["signal_types"],
                office_count=record["office_count"],
                donation_count=record["donation_count"],
                donation_value=record["donation_value"],
                candidacy_count=record["candidacy_count"],
                asset_count=record["asset_count"],
                asset_value=record["asset_value"],
                finance_count=record["finance_count"],
                finance_value=record["finance_value"],
                supplier_contract_count=record["supplier_contract_count"],
                supplier_contract_value=record["supplier_contract_value"],
                person_sanction_count=record.get("person_sanction_count", 0),
                disciplinary_sanction_count=record.get("disciplinary_sanction_count", 0),
                fiscal_responsibility_count=record.get("fiscal_responsibility_count", 0),
                conflict_disclosure_count=record["conflict_disclosure_count"],
                disclosure_reference_count=record["disclosure_reference_count"],
                corporate_activity_disclosure_count=record[
                    "corporate_activity_disclosure_count"
                ],
                donor_vendor_loop_count=record["donor_vendor_loop_count"],
                payment_supervision_count=record.get("payment_supervision_count", 0),
                payment_supervision_company_count=record.get(
                    "payment_supervision_company_count", 0
                ),
                payment_supervision_risk_contract_count=record.get(
                    "payment_supervision_risk_contract_count", 0
                ),
                payment_supervision_discrepancy_contract_count=record.get(
                    "payment_supervision_discrepancy_contract_count", 0
                ),
                payment_supervision_suspension_contract_count=record.get(
                    "payment_supervision_suspension_contract_count", 0
                ),
                payment_supervision_pending_contract_count=record.get(
                    "payment_supervision_pending_contract_count", 0
                ),
                payment_supervision_contract_value=record.get(
                    "payment_supervision_contract_value", 0.0
                ),
                payment_supervision_archive_contract_count=record.get(
                    "payment_supervision_archive_contract_count", 0
                ),
                archive_document_total=record.get("archive_document_total", 0),
                archive_supervision_document_total=record.get(
                    "archive_supervision_document_total", 0
                ),
                archive_payment_document_total=record.get(
                    "archive_payment_document_total", 0
                ),
                archive_assignment_document_total=record.get(
                    "archive_assignment_document_total", 0
                ),
                official_case_bulletin_count=record.get("official_case_bulletin_count", 0),
                official_case_bulletin_titles=list(
                    record.get("official_case_bulletin_titles") or []
                ),
                offices=list(record.get("offices") or []),
                alerts=_build_person_alerts(dict(record)),
            )
            for record in records
        ],
        total=len(records),
    )
    _watchlist_cache[safe_limit] = (now, response)
    return response


@router.get("/watchlist/companies", response_model=PrioritizedCompaniesResponse)
async def prioritized_company_watchlist(
    session: Annotated[AsyncSession, Depends(get_session)],
    limit: int = 12,
) -> PrioritizedCompaniesResponse:
    safe_limit = max(1, min(limit, _MAX_WATCHLIST_LIMIT))
    cached = _company_watchlist_cache.get(safe_limit)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < 300:
        return cached[1]

    snapshot_rows = _load_watchlist_snapshot("companies")
    if snapshot_rows is not None and len(snapshot_rows) >= safe_limit:
        records = snapshot_rows[:safe_limit]
    else:
        try:
            records = await execute_query(
                session,
                "meta_prioritized_companies",
                {
                    "limit": safe_limit,
                    "pattern_min_discrepancy_ratio": settings.pattern_min_discrepancy_ratio,
                    "pattern_split_threshold_value": settings.pattern_split_threshold_value,
                    "pattern_split_min_average_value": settings.pattern_split_min_average_value,
                    "pattern_split_min_total_value": settings.pattern_split_min_total_value,
                    "pattern_split_min_count": settings.pattern_split_min_count,
                },
                timeout=20,
            )
        except Exception:
            if snapshot_rows is None:
                raise
            logger.exception("Falling back to company watchlist snapshot")
            records = snapshot_rows[:safe_limit]
    hide_people = should_hide_person_entities()
    response = PrioritizedCompaniesResponse(
        companies=[
            PrioritizedCompanyResponse(
                entity_id=record["entity_id"],
                name=record["name"],
                document_id=record.get("document_id"),
                suspicion_score=record["suspicion_score"],
                signal_types=record["signal_types"],
                contract_count=record["contract_count"],
                contract_value=record["contract_value"],
                buyer_count=record["buyer_count"],
                sanction_count=record["sanction_count"],
                official_officer_count=record["official_officer_count"],
                official_role_count=record["official_role_count"],
                low_competition_bid_count=record["low_competition_bid_count"],
                low_competition_bid_value=record["low_competition_bid_value"],
                direct_invitation_bid_count=record["direct_invitation_bid_count"],
                funding_overlap_event_count=record["funding_overlap_event_count"],
                funding_overlap_total=record["funding_overlap_total"],
                capacity_mismatch_contract_count=record["capacity_mismatch_contract_count"],
                capacity_mismatch_contract_value=record["capacity_mismatch_contract_value"],
                capacity_mismatch_revenue_ratio=record["capacity_mismatch_revenue_ratio"],
                capacity_mismatch_asset_ratio=record["capacity_mismatch_asset_ratio"],
                execution_gap_contract_count=record["execution_gap_contract_count"],
                execution_gap_invoice_total=record["execution_gap_invoice_total"],
                commitment_gap_contract_count=record["commitment_gap_contract_count"],
                commitment_gap_total=record["commitment_gap_total"],
                suspension_contract_count=record.get("suspension_contract_count", 0),
                suspension_event_count=record.get("suspension_event_count", 0),
                sanctioned_still_receiving_contract_count=record[
                    "sanctioned_still_receiving_contract_count"
                ],
                sanctioned_still_receiving_total=record["sanctioned_still_receiving_total"],
                split_contract_group_count=record["split_contract_group_count"],
                split_contract_total=record["split_contract_total"],
                archive_contract_count=record.get("archive_contract_count", 0),
                archive_document_total=record.get("archive_document_total", 0),
                archive_supervision_contract_count=record.get(
                    "archive_supervision_contract_count", 0
                ),
                archive_supervision_document_total=record.get(
                    "archive_supervision_document_total", 0
                ),
                archive_payment_contract_count=record.get(
                    "archive_payment_contract_count", 0
                ),
                archive_payment_document_total=record.get(
                    "archive_payment_document_total", 0
                ),
                archive_assignment_contract_count=record.get(
                    "archive_assignment_contract_count", 0
                ),
                archive_assignment_document_total=record.get(
                    "archive_assignment_document_total", 0
                ),
                official_names=[] if hide_people else list(record.get("official_names") or []),
                alerts=_build_company_alerts(dict(record)),
            )
            for record in records
        ],
        total=len(records),
    )
    _company_watchlist_cache[safe_limit] = (now, response)
    return response


@router.get("/watchlist/buyers", response_model=PrioritizedBuyersResponse)
async def prioritized_buyer_watchlist(
    session: Annotated[AsyncSession, Depends(get_session)],
    limit: int = 12,
) -> PrioritizedBuyersResponse:
    safe_limit = max(1, min(limit, _MAX_WATCHLIST_LIMIT))
    cached = _buyer_watchlist_cache.get(safe_limit)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < 300:
        return cached[1]

    snapshot_rows = _load_watchlist_snapshot("buyers")
    if snapshot_rows is not None and len(snapshot_rows) >= safe_limit:
        records = snapshot_rows[:safe_limit]
    else:
        try:
            records = await execute_query(
                session,
                "meta_prioritized_buyers",
                {
                    "limit": safe_limit,
                    "pattern_min_discrepancy_ratio": settings.pattern_min_discrepancy_ratio,
                },
                timeout=20,
            )
        except Exception:
            if snapshot_rows is None:
                raise
            logger.exception("Falling back to buyer watchlist snapshot")
            records = snapshot_rows[:safe_limit]
    response = PrioritizedBuyersResponse(
        buyers=[
            PrioritizedBuyerResponse(
                buyer_id=record["buyer_id"],
                buyer_name=record["buyer_name"],
                buyer_document_id=record.get("buyer_document_id"),
                suspicion_score=record["suspicion_score"],
                signal_types=record["signal_types"],
                contract_count=record["contract_count"],
                contract_value=record["contract_value"],
                supplier_count=record["supplier_count"],
                top_supplier_name=record.get("top_supplier_name"),
                top_supplier_document_id=record.get("top_supplier_document_id"),
                top_supplier_share=record["top_supplier_share"],
                low_competition_contract_count=record["low_competition_contract_count"],
                direct_invitation_contract_count=record["direct_invitation_contract_count"],
                sanctioned_supplier_contract_count=record["sanctioned_supplier_contract_count"],
                sanctioned_supplier_value=record["sanctioned_supplier_value"],
                official_overlap_contract_count=record["official_overlap_contract_count"],
                official_overlap_supplier_count=record["official_overlap_supplier_count"],
                capacity_mismatch_supplier_count=record["capacity_mismatch_supplier_count"],
                discrepancy_contract_count=record["discrepancy_contract_count"],
                discrepancy_value=record["discrepancy_value"],
                alerts=_build_buyer_alerts(dict(record)),
            )
            for record in records
        ],
        total=len(records),
    )
    _buyer_watchlist_cache[safe_limit] = (now, response)
    return response


@router.get("/watchlist/territories", response_model=PrioritizedTerritoriesResponse)
async def prioritized_territory_watchlist(
    session: Annotated[AsyncSession, Depends(get_session)],
    limit: int = 12,
) -> PrioritizedTerritoriesResponse:
    safe_limit = max(1, min(limit, _MAX_WATCHLIST_LIMIT))
    cached = _territory_watchlist_cache.get(safe_limit)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < 300:
        return cached[1]

    snapshot_rows = _load_watchlist_snapshot("territories")
    if snapshot_rows is not None and len(snapshot_rows) >= safe_limit:
        records = snapshot_rows[:safe_limit]
    else:
        try:
            records = await execute_query(
                session,
                "meta_prioritized_territories",
                {
                    "limit": safe_limit,
                    "pattern_min_discrepancy_ratio": settings.pattern_min_discrepancy_ratio,
                },
                timeout=20,
            )
        except Exception:
            if snapshot_rows is None:
                raise
            logger.exception("Falling back to territory watchlist snapshot")
            records = snapshot_rows[:safe_limit]
    response = PrioritizedTerritoriesResponse(
        territories=[
            PrioritizedTerritoryResponse(
                territory_id=record["territory_id"],
                territory_name=record["territory_name"],
                department=record["department"],
                municipality=record.get("municipality"),
                suspicion_score=record["suspicion_score"],
                signal_types=record["signal_types"],
                contract_count=record["contract_count"],
                contract_value=record["contract_value"],
                buyer_count=record["buyer_count"],
                supplier_count=record["supplier_count"],
                top_supplier_name=record.get("top_supplier_name"),
                top_supplier_share=record["top_supplier_share"],
                low_competition_contract_count=record["low_competition_contract_count"],
                direct_invitation_contract_count=record["direct_invitation_contract_count"],
                sanctioned_supplier_contract_count=record["sanctioned_supplier_contract_count"],
                sanctioned_supplier_value=record["sanctioned_supplier_value"],
                official_overlap_contract_count=record["official_overlap_contract_count"],
                capacity_mismatch_supplier_count=record["capacity_mismatch_supplier_count"],
                discrepancy_contract_count=record["discrepancy_contract_count"],
                discrepancy_value=record["discrepancy_value"],
                alerts=_build_territory_alerts(dict(record)),
            )
            for record in records
        ],
        total=len(records),
    )
    _territory_watchlist_cache[safe_limit] = (now, response)
    return response
