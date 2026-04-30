"""Join-key classification + promotion policy.

Foundation module for ``coacc_etl.qualification``. Defines:

- :class:`TriageCatalogRow` — the row dataclass written into the signed
  catalog.
- :data:`JOIN_KEY_CLASSES` and :data:`REVERSE_KEY_INDEX` — the curated
  alias dictionaries used for column-name → join-class matching.
- :func:`classify_dataset` — the deterministic recommendation engine
  that turns row counts + found join classes into an ``ingest_class``.

Other ``qualification`` submodules import from here, never the reverse.
"""
# ruff: noqa: E501
from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import asdict, dataclass, field

LOG = logging.getLogger("source_qualification")

CANDIDATE_RECOMMENDATIONS = {
    "candidate",
    "candidate_context",
    "keep_or_fix_deps",
    "review_context",
    "review_or_drop",
    "add_to_registry_or_remove_signal_ref",
    "manual_review",
}

JOIN_KEY_CLASSES: dict[str, list[str]] = {
    "nit": [
        "document_id", "documento_id", "numero_documento", "num_documento",
        "nit", "nit_proveedor", "documento_proveedor", "numero_identificacion",
        "id_contratista", "num_identificacion", "documento_identidad",
        "doc_proveedor", "id_proveedor", "nit_adjudicado",
        "nit_contratista", "num_nit", "nit_empresa", "nit_entidad_contratante",
        "identificacion", "cedula", "cedula_representante_legal",
        "numero_identificacion_representante", "documento_contratista",
        "num_documento_identidad", "tipo_documento_identidad",
        "documento_representante_legal", "nit_verificado",
        # Manual review discoveries (§14 action items):
        "nit_de_la_entidad",
        "nit_entidad_compradora",
        "nit_del_proveedor",
        "nit_grupo",
        "nit_participante",
        "nit",
        "identificacion_del_contratista",
        "productor_nit",
        "numero_nit",
        "n_mero_nit",
        "ccb_nit_inst",
        "numero_documento_representante_legal",
        "n_mero_documento_representante_legal",
        "n_mero_c_dula_representante",
        "n_mero_doc_representante_legal_grupo",
        "numerodeidentificacion",
        "n_mero_de_identificaci_n",
        "identificaci_n",
        "identificacion_funcionario",
        "codigo_grupo",
        "codigo_participante",
        "codigo_proveedor",
        "num_doc_proponente",
        "identific_representante_legal",
    ],
    "contract": [
        "id_contrato", "numero_contrato", "contrato_id", "cod_contrato",
        "reference_contract_number", "num_contrato", "contrato",
        "codigo_contrato", "id_contrato_secop", "numero_contrato_secop",
        # Manual review discoveries:
        "identificadorcontrato",
        "referencia_contrato",
        "id_del_contrato",
        "numero_de_contrato",
    ],
    "process": [
        "id_proceso", "numero_proceso", "proceso_id", "cod_proceso",
        "id_proceso_secop", "codigo_proceso", "numero_proceso_secop",
        # Manual review discoveries:
        "id_del_proceso_de_compra",
        "id_procedimiento",
        "id_adjudicacion",
    ],
    "bpin": [
        "bpin", "codigo_bpin", "numero_bpin", "bpin_proyecto",
        "codigo_bpin_proyecto", "bp_inversion",
        # Manual review discoveries:
        "codigobpin",
        "entidad_bpin",
    ],
    "divipola": [
        "cod_municipio", "cod_departamento", "codigo_divipola",
        "cod_dpto", "cod_mpio", "divipola", "codigo_municipio",
        "codigo_departamento", "cod_mun", "cod_dep", "dpto_mpio",
        "codigo_dane", "cod_dane_municipio", "cod_dane_departamento",
        "dane_cod_municipio", "dane_cod_departamento",
        "codigo_departamento_curso", "codigo_municipio_curso",
        "cod_departamento_entidad", "cod_municipio_entidad",
        "codigo_departamento_entidad", "codigo_municipio_entidad",
        # Manual review discoveries:
        "c_digo_divipola_departamento",
        "c_digo_divipola_municipio",
        "codigodanemunicipiopredio",
        "codigodanedepartamento",
        "codigodaneentidad",
        "codigo_municipio",
        "codigo_departamento",
        "cod_dpto",
        "cod_dane_depto",
        "cod_dane_mun",
        "c_digo_del_departamento_ies",
        "c_digo_del_municipio_ies",
        "c_digo_del_departamento_programa",
        "c_digo_del_municipio_programa",
        "c_digo_municipio",
        "c_digo_departamento",
        "c_digo_de_la_entidad",
        "codigo_municipio_paa",
        "codigo_departamento_paa",
        # Short forms that normalize differently:
        "coddepto",
        "codmpio",
        "sic_codigo_dane",
        "ik_divipola",
        "idmunicipio",
        "iddepartamento",
        "idmunicipioentidad",
    ],
    "entity": [
        "nit_entidad", "cod_entidad", "id_entidad", "codigo_entidad",
        "nit_entidad_contratante", "codigo_entidad_contratante",
        "id_entidad_contratante", "numero_entidad",
        # Manual review discoveries:
        "c_digo_entidad",
        "codigo_entidad",
        "nit_entidad_compradora",
        "codigo_proveedor",
        "dm_institucion_cod_institucion",
        "identificador_empresa",
        "id_ccf",
        "codigo_patrimonio",
        "codentidad",
        "codigo_regional",
        "codigo_centro",
        "codigo_institucion",
        "codigoinstitucion",
        "c_digo_instituci_n",
        "codigoentidad",
        "codigoentidadresponsable",
        "codigoestablecimiento",
        "c_digo_proveedor",
        "cod_institucion",
    ],
}

_normalized_key_cache: dict[str, str] = {}


def _normalize_col(raw: str) -> str:
    if raw in _normalized_key_cache:
        return _normalized_key_cache[raw]
    value = unicodedata.normalize("NFKD", str(raw or ""))
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = value.strip("_")
    _normalized_key_cache[raw] = value
    return value


def _build_reverse_key_index() -> dict[str, str]:
    index: dict[str, str] = {}
    for key_class, aliases in JOIN_KEY_CLASSES.items():
        for alias in aliases:
            norm = _normalize_col(alias)
            index[norm] = key_class
    return index


REVERSE_KEY_INDEX = _build_reverse_key_index()

CATALOG_FIELDS = [
    "dataset_id",
    "name",
    "sector",
    "scope",
    "recommendation",
    "relevance",
    "audit_status",
    "rows",
    "last_update",
    "last_update_days",
    "update_freq",
    "n_columns",
    "n_meaningful_columns",
    "columns_all",
    "join_keys_found",
    "join_key_classes",
    "join_key_columns",
    "sample_join_density",
    "source_refs",
    "origin_refs",
    "signal_refs",
    "probe_notes",
    "url",
]


@dataclass
class TriageCatalogRow:
    dataset_id: str
    name: str
    sector: str
    scope: str
    recommendation: str
    relevance: str
    audit_status: str
    rows: int = -1
    last_update: str = ""
    last_update_days: int = -1
    update_freq: str = ""
    n_columns: int = -1
    n_meaningful_columns: int = -1
    columns_all: str = ""
    join_keys_found: int = 0
    join_key_classes: str = ""
    join_key_columns: str = ""
    sample_join_density: str = ""
    source_refs: str = ""
    origin_refs: str = ""
    signal_refs: str = ""
    probe_notes: list[str] = field(default_factory=list)
    url: str = ""

    def as_csv_row(self) -> dict[str, object]:
        d = asdict(self)
        d["probe_notes"] = "; ".join(self.probe_notes)
        return d


def _find_join_keys(normalized_columns: list[str]) -> tuple[list[str], dict[str, list[str]]]:
    found_classes: dict[str, list[str]] = {}
    for norm_col in normalized_columns:
        primary = REVERSE_KEY_INDEX.get(norm_col)
        if primary is not None:
            found_classes.setdefault(primary, []).append(norm_col)
        secondary = _find_secondary_key_class(norm_col, primary)
        if secondary is not None and secondary != primary:
            found_classes.setdefault(secondary, []).append(norm_col)
    found_keys = list(found_classes.keys())
    return found_keys, found_classes


def _find_secondary_key_class(norm_col: str, primary: str | None) -> str | None:
    if primary == "entity" and "nit" in norm_col:
        return "nit"
    if primary == "nit" and "entidad" in norm_col:
        return "entity"
    if primary is None:
        if "nit" in norm_col and ("proveedor" in norm_col or "contratista" in norm_col or "grupo" in norm_col or "participante" in norm_col):
            return "nit"
        if "codigo" in norm_col or "c_digo" in norm_col:
            if "entidad" in norm_col or "ejecutor" in norm_col:
                return "entity"
            if "municipio" in norm_col or "dane" in norm_col or "depto" in norm_col or "divipola" in norm_col:
                return "divipola"
            if "bpin" in norm_col:
                return "bpin"
    if primary == "divipola" and ("entidad" in norm_col or "ejecutor" in norm_col):
        return "entity"
    return None


def _is_stable_identifier_column(norm_col: str) -> bool:
    has_id_token = (
        norm_col == "id"
        or norm_col.startswith("id_")
        or norm_col.endswith("_id")
        or "_id_" in norm_col
    )
    has_stable_token = has_id_token or any(
        token in norm_col
        for token in (
            "codigo",
            "cod",
            "identificacion",
            "documento",
            "nit",
            "bpin",
            "contrato",
            "proceso",
            "procedimiento",
            "divipola",
            "dane",
        )
    )
    fuzzy_tokens = (
        "nombre",
        "razon_social",
        "razonsocial",
        "direccion",
        "descripcion",
        "objeto",
        "municipio",
        "departamento",
    )
    if not has_stable_token:
        return False
    if any(token in norm_col for token in fuzzy_tokens):
        return any(
            token in norm_col
            for token in (
                "codigo",
                "cod_",
                "cod",
                "id",
                "divipola",
                "dane",
            )
        )
    return True


def _has_id_marker(norm_col: str) -> bool:
    return (
        norm_col == "id"
        or norm_col.startswith("id_")
        or norm_col.endswith("_id")
        or "_id_" in norm_col
        or any(
            token in norm_col
            for token in (
                "codigo",
                "c_digo",
                "cod",
                "numero",
                "n_mero",
                "identificador",
                "reference",
                "referencia",
            )
        )
    )


def _is_valid_llm_join_key(norm_col: str, key_class: str) -> bool:
    """Deterministic guardrail for LLM-suggested join keys.

    The LLM may propose generic administrative codes. Those are not join keys
    unless the column name also matches the specific identity class.
    """
    if not _is_stable_identifier_column(norm_col):
        return False

    generic_reject_tokens = (
        "tipo",
        "clase",
        "estado",
        "categoria",
        "modalidad",
        "serie",
        "subserie",
        "tramo",
    )
    if any(token in norm_col for token in generic_reject_tokens):
        return False

    if key_class == "nit":
        return any(
            token in norm_col
            for token in (
                "nit",
                "documento",
                "identificacion",
                "identificaci_n",
                "cedula",
                "c_dula",
            )
        )

    if key_class == "contract":
        return any(token in norm_col for token in ("contrato", "convenio")) and _has_id_marker(norm_col)

    if key_class == "process":
        return any(
            token in norm_col
            for token in ("proceso", "procedimiento", "adjudicacion")
        ) and _has_id_marker(norm_col)

    if key_class == "bpin":
        return "bpin" in norm_col

    if key_class == "divipola":
        if "divipola" in norm_col or "dane" in norm_col:
            return True
        return any(
            token in norm_col
            for token in ("municipio", "departamento", "depto", "dpto", "mpio")
        ) and _has_id_marker(norm_col)

    if key_class == "entity":
        return any(
            token in norm_col
            for token in (
                "entidad",
                "institucion",
                "instituci_n",
                "proveedor",
                "comprador",
                "contratante",
                "ccf",
                "patrimonio",
                "regional",
                "centro",
                "ejecutor",
            )
        ) and _has_id_marker(norm_col)

    return False


def classify_dataset(row: TriageCatalogRow) -> str:
    if row.audit_status in ("dead_404", "forbidden_403"):
        return "unreachable"

    if row.rows < 0 and row.n_columns < 0:
        return "unknown"

    if row.join_keys_found == 0:
        if row.rows >= 10_000 and row.n_meaningful_columns >= 5:
            return "large_no_join"
        return "no_join"

    has_nit = "nit" in (row.join_key_classes or "")
    has_contract = "contract" in (row.join_key_classes or "")
    has_process = "process" in (row.join_key_classes or "")
    has_entity = "entity" in (row.join_key_classes or "")
    has_bpin = "bpin" in (row.join_key_classes or "")
    has_divipola = "divipola" in (row.join_key_classes or "")

    core_keys = sum(1 for k in (has_nit, has_contract, has_process, has_entity) if k)
    context_keys = sum(1 for k in (has_bpin, has_divipola) if k)

    if row.rows < 0:
        if core_keys >= 1:
            return "schema_core_join"
        if context_keys >= 1:
            return "schema_context_join"

    if core_keys >= 2 and row.rows >= 1_000:
        return "ingest_priority"
    if core_keys >= 1 and row.rows >= 5_000:
        return "ingest"
    if core_keys >= 1:
        return "ingest_if_useful"
    if context_keys >= 1 and row.rows >= 1_000:
        return "context_enrichment"
    if context_keys >= 1:
        return "context_if_useful"

    return "weak_join"
