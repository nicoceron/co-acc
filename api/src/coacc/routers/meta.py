import json
import time
from dataclasses import replace
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from neo4j import AsyncSession

from coacc.config import settings
from coacc.dependencies import get_session
from coacc.models.dashboard import (
    RiskAlertResponse,
    SuspiciousBuyerResponse,
    SuspiciousBuyersResponse,
    SuspiciousCompaniesResponse,
    SuspiciousCompanyResponse,
    SuspiciousPeopleResponse,
    SuspiciousPersonResponse,
    SuspiciousTerritoriesResponse,
    SuspiciousTerritoryResponse,
)
from coacc.services.neo4j_service import execute_query, execute_query_single
from coacc.services.public_guard import should_hide_person_entities
from coacc.services.source_registry import load_source_registry, source_registry_summary

router = APIRouter(prefix="/api/v1/meta", tags=["meta"])

_stats_cache: dict[str, Any] | None = None
_stats_cache_time: float = 0.0
_watchlist_cache: dict[int, tuple[float, SuspiciousPeopleResponse]] = {}
_company_watchlist_cache: dict[int, tuple[float, SuspiciousCompaniesResponse]] = {}
_buyer_watchlist_cache: dict[int, tuple[float, SuspiciousBuyersResponse]] = {}
_territory_watchlist_cache: dict[int, tuple[float, SuspiciousTerritoriesResponse]] = {}
_snapshot_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_SNAPSHOT_DIR = Path(__file__).resolve().parent.parent / "data" / "watchlists"


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


def _build_person_alerts(record: dict[str, Any]) -> list[RiskAlertResponse]:
    alerts: list[RiskAlertResponse] = []
    document_id = record.get("document_id")

    office_count = int(record.get("office_count") or 0)
    supplier_contract_count = int(record.get("supplier_contract_count") or 0)
    donor_vendor_loop_count = int(record.get("donor_vendor_loop_count") or 0)
    donation_count = int(record.get("donation_count") or 0)
    disclosure_reference_count = int(record.get("disclosure_reference_count") or 0)
    conflict_disclosure_count = int(record.get("conflict_disclosure_count") or 0)
    corporate_activity_disclosure_count = int(
        record.get("corporate_activity_disclosure_count") or 0
    )

    if office_count > 0 and supplier_contract_count > 0:
        alerts.append(
            _risk_alert(
                alert_type="public_official_supplier_overlap",
                finding_class="incompatibility",
                severity_score=min(95, 55 + office_count * 5 + supplier_contract_count * 3),
                confidence_tier="A",
                reason_text=(
                    "El mismo documento aparece en cargos públicos y también como proveedor"
                    f" con {supplier_contract_count} contrato(s) registrados."
                ),
                evidence_refs=_clean_refs(
                    f"documento:{document_id}" if document_id else None,
                    f"cargos:{office_count}",
                    f"contratos:{supplier_contract_count}",
                ),
                source_list=["SIGEP II", "SECOP / SECOP II"],
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


def _build_company_alerts(record: dict[str, Any]) -> list[RiskAlertResponse]:
    alerts: list[RiskAlertResponse] = []
    document_id = record.get("document_id")

    sanction_count = int(record.get("sanction_count") or 0)
    official_officer_count = int(record.get("official_officer_count") or 0)
    funding_overlap_event_count = int(record.get("funding_overlap_event_count") or 0)
    execution_gap_contract_count = int(record.get("execution_gap_contract_count") or 0)
    commitment_gap_contract_count = int(record.get("commitment_gap_contract_count") or 0)
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
                source_list=["PACO / SIRI", "SECOP sanciones", "SECOP / SECOP II"],
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
                    "Los contratos asociados muestran brechas entre factura, compromiso"
                    " y avance de ejecución."
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
                ),
                source_list=[
                    "SECOP II compromisos",
                    "SECOP II facturas",
                    "SECOP / SECOP II contratos",
                ],
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

    return sorted(alerts, key=lambda alert: alert.severity_score, reverse=True)[:3]


def _build_buyer_alerts(record: dict[str, Any]) -> list[RiskAlertResponse]:
    alerts: list[RiskAlertResponse] = []
    buyer_ref = record.get("buyer_document_id") or record.get("buyer_name")
    top_supplier_share = float(record.get("top_supplier_share") or 0.0)
    discrepancy_contract_count = int(record.get("discrepancy_contract_count") or 0)
    sanctioned_supplier_contract_count = int(record.get("sanctioned_supplier_contract_count") or 0)
    official_overlap_supplier_count = int(record.get("official_overlap_supplier_count") or 0)
    capacity_mismatch_supplier_count = int(record.get("capacity_mismatch_supplier_count") or 0)
    low_competition_contract_count = int(record.get("low_competition_contract_count") or 0)
    direct_invitation_contract_count = int(record.get("direct_invitation_contract_count") or 0)

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
                source_list=["PACO / SIRI", "SECOP sanciones", "SECOP / SECOP II"],
                what_is_unproven=(
                    "La exposición no prueba una irregularidad del comprador sin verificar"
                    " la vigencia de la sanción y el contexto del proceso."
                ),
                next_step=(
                    "Revise la vigencia de cada sanción y la trazabilidad del proceso de compra."
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
                reason_text=(
                    "Los contratos del comprador muestran brechas entre compromiso,"
                    " facturación y avance de ejecución."
                ),
                evidence_refs=_clean_refs(
                    f"comprador:{buyer_ref}" if buyer_ref else None,
                    f"contratos_con_brecha:{discrepancy_contract_count}",
                    f"valor_brecha:{_compact_float(record.get('discrepancy_value'))}",
                ),
                source_list=[
                    "SECOP II compromisos",
                    "SECOP II facturas",
                    "SECOP / SECOP II contratos",
                ],
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
    uses_project_snapshot = (
        int(record.get("supplier_count") or 0) == 0
        and int(record.get("buyer_count") or 0) > 0
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
                source_list=["PACO / SIRI", "SECOP sanciones", "SECOP / SECOP II"],
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
                        f"En {territory_ref} hay contratos con señales de brecha entre"
                        " compromiso, factura y ejecución."
                    )
                ),
                evidence_refs=_clean_refs(
                    f"territorio:{territory_ref}" if territory_ref else None,
                    f"contratos_con_brecha:{discrepancy_contract_count}",
                    f"valor_brecha:{_compact_float(record.get('discrepancy_value'))}",
                ),
                source_list=(
                    ["MapaInversiones", "PTE / SGR"]
                    if uses_project_snapshot
                    else [
                        "SECOP II compromisos",
                        "SECOP II facturas",
                        "SECOP / SECOP II contratos",
                    ]
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
        "embargo_count": record["embargo_count"] if record else 0,
        "education_count": record["education_count"] if record else 0,
        "convenio_count": record["convenio_count"] if record else 0,
        "laborstats_count": record["laborstats_count"] if record else 0,
        "offshore_entity_count": record["offshore_entity_count"] if record else 0,
        "offshore_officer_count": record["offshore_officer_count"] if record else 0,
        "global_pep_count": record["global_pep_count"] if record else 0,
        "cvm_proceeding_count": record["cvm_proceeding_count"] if record else 0,
        "expense_count": record["expense_count"] if record else 0,
        "pep_record_count": record["pep_record_count"] if record else 0,
        "expulsion_count": record["expulsion_count"] if record else 0,
        "leniency_count": record["leniency_count"] if record else 0,
        "international_sanction_count": record["international_sanction_count"] if record else 0,
        "gov_card_expense_count": record["gov_card_expense_count"] if record else 0,
        "gov_travel_count": record["gov_travel_count"] if record else 0,
        "bid_count": record["bid_count"] if record else 0,
        "fund_count": record["fund_count"] if record else 0,
        "dou_act_count": record["dou_act_count"] if record else 0,
        "tax_waiver_count": record["tax_waiver_count"] if record else 0,
        "municipal_finance_count": record["municipal_finance_count"] if record else 0,
        "declared_asset_count": record["declared_asset_count"] if record else 0,
        "party_membership_count": record["party_membership_count"] if record else 0,
        "barred_ngo_count": record["barred_ngo_count"] if record else 0,
        "bcb_penalty_count": record["bcb_penalty_count"] if record else 0,
        "labor_movement_count": record["labor_movement_count"] if record else 0,
        "legal_case_count": record["legal_case_count"] if record else 0,
        "judicial_case_count": record["judicial_case_count"] if record else 0,
        "source_document_count": record.get("source_document_count", 0) if record else 0,
        "ingestion_run_count": record.get("ingestion_run_count", 0) if record else 0,
        "temporal_violation_count": record.get("temporal_violation_count", 0) if record else 0,
        "cpi_count": record["cpi_count"] if record else 0,
        "inquiry_requirement_count": record["inquiry_requirement_count"] if record else 0,
        "inquiry_session_count": record["inquiry_session_count"] if record else 0,
        "municipal_bid_count": record["municipal_bid_count"] if record else 0,
        "municipal_contract_count": record["municipal_contract_count"] if record else 0,
        "municipal_gazette_act_count": record["municipal_gazette_act_count"] if record else 0,
        "data_sources": source_summary["universe_v1_sources"],
        "implemented_sources": source_summary["implemented_sources"],
        "loaded_sources": source_summary["loaded_sources"],
        "healthy_sources": source_summary["healthy_sources"],
        "stale_sources": source_summary["stale_sources"],
        "blocked_external_sources": source_summary["blocked_external_sources"],
        "quality_fail_sources": source_summary["quality_fail_sources"],
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


@router.get("/watchlist/people", response_model=SuspiciousPeopleResponse)
async def suspicious_people_watchlist(
    session: Annotated[AsyncSession, Depends(get_session)],
    limit: int = 12,
) -> SuspiciousPeopleResponse:
    if should_hide_person_entities():
        return SuspiciousPeopleResponse(people=[], total=0)

    safe_limit = max(1, min(limit, 25))
    cached = _watchlist_cache.get(safe_limit)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < 300:
        return cached[1]

    records = await execute_query(
        session,
        "meta_suspicious_people",
        {"limit": safe_limit},
        timeout=20,
    )
    response = SuspiciousPeopleResponse(
        people=[
            SuspiciousPersonResponse(
                entity_id=record["entity_id"],
                name=record["name"],
                document_id=record.get("document_id"),
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
                conflict_disclosure_count=record["conflict_disclosure_count"],
                disclosure_reference_count=record["disclosure_reference_count"],
                corporate_activity_disclosure_count=record[
                    "corporate_activity_disclosure_count"
                ],
                donor_vendor_loop_count=record["donor_vendor_loop_count"],
                offices=list(record.get("offices") or []),
                alerts=_build_person_alerts(record),
            )
            for record in records
        ],
        total=len(records),
    )
    _watchlist_cache[safe_limit] = (now, response)
    return response


@router.get("/watchlist/companies", response_model=SuspiciousCompaniesResponse)
async def suspicious_company_watchlist(
    session: Annotated[AsyncSession, Depends(get_session)],
    limit: int = 12,
) -> SuspiciousCompaniesResponse:
    safe_limit = max(1, min(limit, 25))
    cached = _company_watchlist_cache.get(safe_limit)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < 300:
        return cached[1]

    records = await execute_query(
        session,
        "meta_suspicious_companies",
        {
            "limit": safe_limit,
            "pattern_min_discrepancy_ratio": settings.pattern_min_discrepancy_ratio,
        },
        timeout=20,
    )
    hide_people = should_hide_person_entities()
    response = SuspiciousCompaniesResponse(
        companies=[
            SuspiciousCompanyResponse(
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
                official_names=[] if hide_people else list(record.get("official_names") or []),
                alerts=_build_company_alerts(record),
            )
            for record in records
        ],
        total=len(records),
    )
    _company_watchlist_cache[safe_limit] = (now, response)
    return response


@router.get("/watchlist/buyers", response_model=SuspiciousBuyersResponse)
async def suspicious_buyer_watchlist(
    session: Annotated[AsyncSession, Depends(get_session)],
    limit: int = 12,
) -> SuspiciousBuyersResponse:
    safe_limit = max(1, min(limit, 25))
    cached = _buyer_watchlist_cache.get(safe_limit)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < 300:
        return cached[1]

    snapshot_rows = _load_watchlist_snapshot("buyers")
    if snapshot_rows is not None:
        selected = snapshot_rows[:safe_limit]
        response = SuspiciousBuyersResponse(
            buyers=[
                SuspiciousBuyerResponse(
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
                    alerts=_build_buyer_alerts(record),
                )
                for record in selected
            ],
            total=len(snapshot_rows),
        )
        _buyer_watchlist_cache[safe_limit] = (now, response)
        return response

    records = await execute_query(
        session,
        "meta_suspicious_buyers",
        {
            "limit": safe_limit,
            "pattern_min_discrepancy_ratio": settings.pattern_min_discrepancy_ratio,
        },
        timeout=20,
    )
    response = SuspiciousBuyersResponse(
        buyers=[
            SuspiciousBuyerResponse(
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
                alerts=_build_buyer_alerts(record),
            )
            for record in records
        ],
        total=len(records),
    )
    _buyer_watchlist_cache[safe_limit] = (now, response)
    return response


@router.get("/watchlist/territories", response_model=SuspiciousTerritoriesResponse)
async def suspicious_territory_watchlist(
    session: Annotated[AsyncSession, Depends(get_session)],
    limit: int = 12,
) -> SuspiciousTerritoriesResponse:
    safe_limit = max(1, min(limit, 25))
    cached = _territory_watchlist_cache.get(safe_limit)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < 300:
        return cached[1]

    snapshot_rows = _load_watchlist_snapshot("territories")
    if snapshot_rows is not None:
        selected = snapshot_rows[:safe_limit]
        response = SuspiciousTerritoriesResponse(
            territories=[
                SuspiciousTerritoryResponse(
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
                    alerts=_build_territory_alerts(record),
                )
                for record in selected
            ],
            total=len(snapshot_rows),
        )
        _territory_watchlist_cache[safe_limit] = (now, response)
        return response

    records = await execute_query(
        session,
        "meta_suspicious_territories",
        {
            "limit": safe_limit,
            "pattern_min_discrepancy_ratio": settings.pattern_min_discrepancy_ratio,
        },
        timeout=20,
    )
    response = SuspiciousTerritoriesResponse(
        territories=[
            SuspiciousTerritoryResponse(
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
                alerts=_build_territory_alerts(record),
            )
            for record in records
        ],
        total=len(records),
    )
    _territory_watchlist_cache[safe_limit] = (now, response)
    return response
