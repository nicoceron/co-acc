from pydantic import BaseModel

from coacc.models.entity import SourceAttribution


class PatternResult(BaseModel):
    pattern_id: str
    pattern_name: str
    description: str
    data: dict[str, str | float | int | bool | list[str] | None]
    entity_ids: list[str]
    sources: list[SourceAttribution]
    exposure_tier: str = "public_safe"
    intelligence_tier: str = "community"


class PatternResponse(BaseModel):
    entity_id: str | None
    patterns: list[PatternResult]
    total: int


PATTERN_METADATA: dict[str, dict[str, str]] = {
    "sanctioned_still_receiving": {
        "name_es": "Coocurrencia: sanción y contrato",
        "name_en": "Co-occurrence: sanction and contract",
        "desc_es": (
            "Contrato con fecha dentro de la ventana registrada de sanción"
            " de la empresa (PACO/SECOP)"
        ),
        "desc_en": (
            "Contract date within the company's recorded sanction window"
            " (PACO/SECOP)"
        ),
    },
    "sanctioned_supplier_record": {
        "name_es": "Proveedor con historial de sanción",
        "name_en": "Supplier with sanction history",
        "desc_es": (
            "Proveedor con sanciones registradas en fuentes públicas"
            " y exposición contractual asociada"
        ),
        "desc_en": (
            "Supplier with sanctions recorded in public sources"
            " and associated contract exposure"
        ),
    },
    "contract_concentration": {
        "name_es": "Concentración de proveedor por entidad",
        "name_en": "Supplier concentration by agency",
        "desc_es": (
            "Participación del proveedor por encima del umbral configurado"
            " en el gasto contractual de la entidad"
        ),
        "desc_en": (
            "Supplier share above configured threshold"
            " in an agency's contract spend"
        ),
    },
    "debtor_contracts": {
        "name_es": "Coocurrencia: deudor fiscal y contratos",
        "name_en": "Co-occurrence: tax debtor and contracts",
        "desc_es": (
            "Empresa con registros de deudas fiscales"
            " y recurrencia de contratos públicos"
        ),
        "desc_en": (
            "Company with recorded tax debts"
            " and recurring public contracts"
        ),
    },
    "amendment_beneficiary_contracts": {
        "name_es": "Coocurrencia: adición/convenio y contratos",
        "name_en": "Co-occurrence: amendment/grant and contracts",
        "desc_es": (
            "Empresa beneficiada por adiciones o convenios"
            " que también posee contratos públicos registrados"
        ),
        "desc_en": (
            "Company benefited by amendments or grants"
            " that also holds recorded public contracts"
        ),
    },
    "split_contracts_below_threshold": {
        "name_es": "Recurrencia de contratos bajo el tope",
        "name_en": "Recurring contracts below threshold",
        "desc_es": (
            "Múltiples contratos con la misma entidad y objeto,"
            " en el mismo mes, bajo el tope de contratación directa"
        ),
        "desc_en": (
            "Multiple contracts with same agency and object,"
            " in the same month, below direct award threshold"
        ),
    },
    "inexigibility_recurrence": {
        "name_es": "Recurrencia de inexigibilidad",
        "name_en": "Recurring inexigibility",
        "desc_es": (
            "Proveedor recurrente en procesos de inexigibilidad"
            " con la misma entidad y objeto"
        ),
        "desc_en": (
            "Recurring supplier in inexigibility processes"
            " for the same agency and object"
        ),
    },
    "public_official_supplier_overlap": {
        "name_es": "Proveedor con directivo en cargo público",
        "name_en": "Supplier with public-office officer",
        "desc_es": (
            "Empresa proveedora cuyo directivo también aparece"
            " en registros activos de cargo o salario público (SIGEP)"
        ),
        "desc_en": (
            "Supplier company whose officer also appears"
            " in active public-office or payroll records (SIGEP)"
        ),
    },
    "low_competition_bidding": {
        "name_es": "Recurrencia en baja competencia",
        "name_en": "Recurring low-competition bidding",
        "desc_es": (
            "Empresa concentrada en procesos con uno o pocos oferentes"
            " o invitaciones directas recurrentes"
        ),
        "desc_en": (
            "Company concentrated in one- or two-bidder processes"
            " or recurring direct invitations"
        ),
    },
    "invoice_execution_gap": {
        "name_es": "Facturación sin ejecución compatible",
        "name_en": "Invoices without matching execution",
        "desc_es": (
            "Contratos con facturación relevante, pero avance físico"
            " registrado muy bajo (Elefantes Blancos)"
        ),
        "desc_en": (
            "Contracts with meaningful invoicing but very low"
            " recorded execution progress (Stalled projects)"
        ),
    },
    "invoice_commitment_gap": {
        "name_es": "Factura por encima del compromiso",
        "name_en": "Invoice above commitment",
        "desc_es": (
            "Valor facturado por encima del valor comprometido"
            " en la cadena presupuestal del contrato"
        ),
        "desc_en": (
            "Invoiced value exceeds the committed value"
            " recorded in the contract budget chain"
        ),
    },
    "funding_spike_then_awards": {
        "name_es": "Traslape entre ejecución pública y contratos",
        "name_en": "Upstream public-funding overlap",
        "desc_es": (
            "Proveedor que aparece en ejecución de gastos de regalías (SGR)"
            " y también concentra contratos públicos registrados"
        ),
        "desc_en": (
            "Supplier that appears in royalty-system expense execution (SGR)"
            " while also concentrating recorded public contracts"
        ),
    },
    "company_capacity_mismatch": {
        "name_es": "Capacidad financiera incompatible",
        "name_en": "Company-capacity mismatch",
        "desc_es": (
            "Volumen contractual muy por encima de los ingresos operativos"
            " o de los activos reportados por la empresa"
        ),
        "desc_en": (
            "Contract exposure materially exceeds the company's"
            " reported operating revenue or asset base"
        ),
    },
    "donor_official_vendor_loop": {
        "name_es": "Ciclo donante-servidor-proveedor",
        "name_en": "Donor-official-vendor loop",
        "desc_es": (
            "El mismo documento aparece en cargo público, donaciones electorales"
            " y contratación pública como proveedor"
        ),
        "desc_en": (
            "The same ID appears in public office, election donations,"
            " and procurement supplier records"
        ),
    },
    "disclosure_risk_stack": {
        "name_es": "Riesgos acumulados en declaraciones",
        "name_en": "Disclosure-driven risk stack",
        "desc_es": (
            "Declaraciones de conflicto y bienes mencionan empresas,"
            " intereses privados o roles legales al mismo tiempo"
            " que la persona aparece como proveedora"
        ),
        "desc_en": (
            "Conflict and asset disclosures mention companies, private interests,"
            " or legal roles while the same person"
            " also appears as a procurement supplier"
        ),
    },
}
