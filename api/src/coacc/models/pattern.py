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
    "sensitive_public_official_supplier_overlap": {
        "name_es": "Proveedor con directivo en cargo sensible",
        "name_en": "Supplier with officer in a sensitive public role",
        "desc_es": (
            "Empresa proveedora vinculada a personas que figuran"
            " en cargos sensibles a la corrupción o al control presupuestal"
        ),
        "desc_en": (
            "Supplier company linked to people who appear"
            " in roles flagged as corruption-sensitive or budget-control sensitive"
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
    "company_donor_vendor_overlap": {
        "name_es": "Empresa donante con contratación pública",
        "name_en": "Company donor with public contracts",
        "desc_es": (
            "La misma empresa aparece como donante electoral"
            " y también como contratista del Estado"
        ),
        "desc_en": (
            "The same company appears in campaign donations"
            " and also as a public contractor"
        ),
    },
    "shared_officer_supplier_network": {
        "name_es": "Red de proveedores con directivo compartido",
        "name_en": "Supplier network with shared officer",
        "desc_es": (
            "La empresa comparte representante legal o directivo"
            " con otras proveedoras que también reciben contratos públicos"
        ),
        "desc_en": (
            "The company shares an officer or legal representative"
            " with other suppliers that also receive public contracts"
        ),
    },
    "public_money_channel_stacking": {
        "name_es": "Apilamiento de canales de dinero público",
        "name_en": "Public-money channel stacking",
        "desc_es": (
            "La misma empresa aparece en varios canales de gasto público:"
            " contratos, regalías, beneficios, administración de recursos o salud"
        ),
        "desc_en": (
            "The same company appears across multiple public-spending channels:"
            " contracts, royalties, benefit flows, managed funds, or health operations"
        ),
    },
    "sanctioned_health_operator_overlap": {
        "name_es": "Operador de salud sancionado con contratación",
        "name_en": "Sanctioned health operator with contracts",
        "desc_es": (
            "Prestador u operador de salud con sanciones públicas"
            " y exposición contractual estatal"
        ),
        "desc_en": (
            "Health provider or operator with public sanctions"
            " and recurring state-contract exposure"
        ),
    },
    "contract_suspension_stacking": {
        "name_es": "Suspensiones repetidas de contratos",
        "name_en": "Repeated contract suspensions",
        "desc_es": (
            "La empresa aparece en contratos con suspensiones reiteradas,"
            " un patrón sensible cuando coincide con facturación o cambios postadjudicación"
        ),
        "desc_en": (
            "The company appears in contracts with repeated suspensions,"
            " a sensitive pattern when paired with invoicing or post-award changes"
        ),
    },
    "payment_supervision_risk_stack": {
        "name_es": "Supervisión de pagos sobre contratos riesgosos",
        "name_en": "Payment supervision over risky contracts",
        "desc_es": (
            "La persona figura en supervisión de pagos o interventoría de contratos"
            " que también muestran brechas de ejecución, suspensiones o pagos pendientes"
        ),
        "desc_en": (
            "The person appears in payment-supervision or interventor roles on contracts"
            " that also show execution gaps, suspensions, or pending payments"
        ),
    },
    "sanctioned_person_exposure_stack": {
        "name_es": "Sanciones oficiales con exposición pública",
        "name_en": "Official sanctions with public exposure",
        "desc_es": (
            "La persona registra sanciones disciplinarias, fiscales o de control"
            " y además aparece en contratación, nómina pública, actividad electoral"
            " o supervisión de pagos"
        ),
        "desc_en": (
            "The person has disciplinary, fiscal, or control sanctions"
            " and also appears in procurement, public payroll, election activity,"
            " or payment-supervision roles"
        ),
    },
    "interadministrative_channel_stacking": {
        "name_es": "Apilamiento de convenios interadministrativos",
        "name_en": "Interadministrative-channel stacking",
        "desc_es": (
            "La empresa aparece como contraparte en convenios interadministrativos"
            " y también como contratista regular, con señales adicionales de riesgo"
        ),
        "desc_en": (
            "The company appears as an interadministrative-agreement counterparty"
            " and also as a regular contractor, with additional risk overlays"
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
