from __future__ import annotations

import csv
import json
import os
import re
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import httpx
from neo4j import GraphDatabase

from coacc_etl.bogota_secop import build_in_clauses
from coacc_etl.pipelines.colombia_shared import clean_name, clean_text, normalize_column_name
from coacc_etl.transforms import strip_document

FALSE_ID_CANDIDATES = {"open-data", "nine-year"}
DEFAULT_DOC_PATHS = (
    Path("docs/datos_abiertos_research_2026-03-18.md"),
    Path("docs/colombia_corruption_practices_research_2026-03-19.md"),
    Path("docs/payroll_pensions_benchmarks_2026-03-21.md"),
)
DEFAULT_REGISTRY_PATH = Path("docs/source_registry_co_v1.csv")

CANONICAL_FAMILY_PATTERNS: dict[str, tuple[str, ...]] = {
    "contract_id": (
        "id_contrato",
        "id_del_contrato",
        "id_contracto",
        "identificador_del_contrato",
        "numero_constancia",
        "constancia",
    ),
    "process_id": (
        "id_proceso",
        "id_del_proceso",
        "proceso_de_compra",
        "referencia_del_proceso",
        "numero_del_proceso",
        "proceso",
    ),
    "company_id": (
        "nit",
        "numero_de_identificacion",
        "numero_identificacion",
        "nro_identificacion",
        "documento_proveedor",
        "nit_proveedor",
        "codigo_proveedor",
        "identificacion_del_contratista",
        "matricula",
    ),
    "person_id": (
        "cedula",
        "cc",
        "numero_de_identificacion",
        "numero_documento",
        "numeroidentificacion",
        "identificacion_funcionario",
        "identificacion_representante_legal",
        "n_mero_doc_representante_legal",
        "nuip",
    ),
    "company_name": (
        "razon_social",
        "nombre_entidad",
        "nombre_proveedor",
        "proveedor",
        "contratista",
        "nombre",
    ),
    "person_name": (
        "nombre_completo",
        "nombre_persona",
        "nombre_representante_legal",
        "primer_nombre",
        "segundo_nombre",
        "primer_apellido",
        "segundo_apellido",
    ),
    "bpin": ("bpin", "codigo_bpin"),
    "territory": (
        "departamento",
        "municipio",
        "ciudad",
        "codigo_dane",
        "divipola",
        "ubica",
    ),
    "administrative_file": ("expediente", "radicado", "resolucion", "providencia"),
    "payment_or_invoice": ("factura", "cufe", "pago", "radicado", "ticket"),
}

EXACT_FAMILIES = {"contract_id", "process_id", "company_id", "person_id", "bpin"}
WEAK_NAME_FAMILIES = {"company_name", "person_name"}
FAMILY_PRIORITY = ("contract_id", "process_id", "company_id", "person_id", "bpin")


@dataclass
class ColumnProbe:
    actual_name: str
    normalized_name: str
    non_empty_sample_values: int
    sample_overlap: int
    live_probe_hits: int | None = None


@dataclass
class DatasetProbeResult:
    dataset_id: str
    title: str
    description: str
    row_count: int | None
    updated_at: str | None
    columns: list[str]
    key_families: dict[str, list[str]]
    probes: dict[str, list[ColumnProbe]]
    recommendation: str
    reason: str
    errors: list[str] = field(default_factory=list)


@dataclass
class UniverseIndexes:
    contract_ids: set[str]
    process_ids: set[str]
    company_ids: set[str]
    person_ids: set[str]
    bpin_codes: set[str]
    company_names: set[str]
    person_names: set[str]
    contract_seed: list[str]
    process_seed: list[str]
    company_seed: list[str]
    person_seed: list[str]
    bpin_seed: list[str]

    def full_set_for_family(self, family: str) -> set[str]:
        return {
            "contract_id": self.contract_ids,
            "process_id": self.process_ids,
            "company_id": self.company_ids,
            "person_id": self.person_ids,
            "bpin": self.bpin_codes,
            "company_name": self.company_names,
            "person_name": self.person_names,
        }.get(family, set())

    def seed_values_for_family(self, family: str) -> list[str]:
        return {
            "contract_id": self.contract_seed,
            "process_id": self.process_seed,
            "company_id": self.company_seed,
            "person_id": self.person_seed,
            "bpin": self.bpin_seed,
        }.get(family, [])


def _normalize_exact_value(raw: object, family: str) -> str:
    value = clean_text(raw)
    if not value:
        return ""
    if family in {"company_id", "person_id"}:
        return strip_document(value)
    if family in {"contract_id", "process_id"}:
        return value.upper()
    if family == "bpin":
        return strip_document(value)
    if family in WEAK_NAME_FAMILIES:
        return clean_name(value)
    return normalize_column_name(value)


def _column_matches_pattern(normalized: str, pattern: str) -> bool:
    if not normalized or not pattern:
        return False
    if normalized == pattern:
        return True
    if "_" in pattern:
        return f"_{pattern}_" in f"_{normalized}_"
    if pattern == "matricula":
        return normalized.startswith("matricula_") or normalized.endswith("_matricula")
    tokenized = normalized.split("_")
    return pattern in tokenized


def infer_key_families(columns: list[str]) -> dict[str, list[str]]:
    families: dict[str, list[str]] = defaultdict(list)
    normalized_columns = [normalize_column_name(column) for column in columns]
    for original, normalized in zip(columns, normalized_columns, strict=True):
        for family, patterns in CANONICAL_FAMILY_PATTERNS.items():
            if any(_column_matches_pattern(normalized, pattern) for pattern in patterns):
                families[family].append(original)
    return dict(families)


def extract_candidate_dataset_ids(
    *,
    doc_paths: tuple[Path, ...] = DEFAULT_DOC_PATHS,
    registry_path: Path = DEFAULT_REGISTRY_PATH,
) -> list[str]:
    discovered: set[str] = set()
    pattern = re.compile(r"\b[a-z0-9]{4}-[a-z0-9]{4}\b")
    for path in doc_paths:
        if not path.exists():
            continue
        discovered.update(pattern.findall(path.read_text(encoding="utf-8")))

    implemented: set[str] = set()
    if registry_path.exists():
        with registry_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                url = clean_text(row.get("primary_url"))
                if "/d/" not in url:
                    continue
                implemented.add(url.rsplit("/d/", 1)[1].split("/", 1)[0])

    candidates = sorted(
        dataset_id
        for dataset_id in discovered
        if dataset_id not in implemented and dataset_id not in FALSE_ID_CANDIDATES
    )
    return candidates


def _load_csv_column_set(path: Path, column_names: tuple[str, ...], *, family: str) -> set[str]:
    if not path.exists():
        return set()

    values: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return values
        field_lookup = {
            normalize_column_name(field): field
            for field in reader.fieldnames
            if clean_text(field)
        }
        actual_fields = [
            field_lookup[normalize_column_name(column)]
            for column in column_names
            if normalize_column_name(column) in field_lookup
        ]
        if not actual_fields:
            return values
        for row in reader:
            for field in actual_fields:
                normalized = _normalize_exact_value(row.get(field), family)
                if normalized:
                    values.add(normalized)
    return values


def _load_csv_counter(path: Path, column_names: tuple[str, ...], *, family: str) -> Counter[str]:
    values: Counter[str] = Counter()
    if not path.exists():
        return values

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return values
        field_lookup = {
            normalize_column_name(field): field
            for field in reader.fieldnames
            if clean_text(field)
        }
        actual_fields = [
            field_lookup[normalize_column_name(column)]
            for column in column_names
            if normalize_column_name(column) in field_lookup
        ]
        if not actual_fields:
            return values
        for row in reader:
            for field in actual_fields:
                normalized = _normalize_exact_value(row.get(field), family)
                if normalized:
                    values[normalized] += 1
    return values


def _neo4j_password(repo_root: Path) -> str:
    env_path = repo_root / ".env"
    if not env_path.exists():
        return os.getenv("NEO4J_PASSWORD", "")
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("NEO4J_PASSWORD="):
            return line.split("=", 1)[1].strip()
    return os.getenv("NEO4J_PASSWORD", "")


def _graph_value_set(
    repo_root: Path,
    *,
    label: str,
    properties: tuple[str, ...],
    normalizer_family: str,
) -> set[str]:
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    password = _neo4j_password(repo_root)
    driver = GraphDatabase.driver(uri, auth=("neo4j", password))
    property_expr = ", ".join(f"n.{prop}" for prop in properties)
    query = (
        f"MATCH (n:{label}) "
        f"RETURN DISTINCT coalesce({property_expr}) AS value"
    )
    values: set[str] = set()
    with driver.session(database=os.getenv("NEO4J_DATABASE", "neo4j")) as session:
        for record in session.run(query):
            normalized = _normalize_exact_value(record["value"], normalizer_family)
            if normalized:
                values.add(normalized)
    driver.close()
    return values


def build_universe_indexes(repo_root: Path) -> UniverseIndexes:
    data_root = repo_root / "data"

    contract_ids = _load_csv_column_set(
        data_root / "secop_ii_contracts" / "contract_summary_map.csv",
        ("contract_id",),
        family="contract_id",
    )

    process_ids = set()
    for path, columns in (
        (data_root / "secop_ii_contracts" / "secop_ii_contracts.csv", ("proceso_de_compra",)),
        (data_root / "secop_ii_processes" / "secop_ii_processes.csv", ("id_del_proceso",)),
        (data_root / "secop_cdp_requests" / "secop_cdp_requests.csv", ("id_proceso",)),
        (data_root / "secop_process_bpin" / "secop_process_bpin.csv", ("id_proceso",)),
    ):
        process_ids.update(_load_csv_column_set(path, columns, family="process_id"))

    bpin_codes = set()
    for path, columns in (
        (data_root / "secop_process_bpin" / "secop_process_bpin.csv", ("codigo_bpin",)),
        (data_root / "secop_cdp_requests" / "secop_cdp_requests.csv", ("bpin_codigo",)),
    ):
        bpin_codes.update(_load_csv_column_set(path, columns, family="bpin"))

    company_counter = Counter[str]()
    for path, columns in (
        (
            data_root / "secop_ii_contracts" / "secop_ii_contracts.csv",
            ("documento_proveedor", "nit_entidad", "identificaci_n_representante_legal"),
        ),
        (
            data_root / "secop_suppliers" / "secop_suppliers.csv",
            ("nit", "n_mero_doc_representante_legal"),
        ),
        (
            data_root / "secop_interadmin_agreements" / "secop_interadmin_agreements.csv",
            ("nit_contratista", "nit_entidad"),
        ),
        (
            data_root / "fiscal_responsibility" / "fiscal_responsibility.csv",
            ("numero_de_identificacion",),
        ),
    ):
        company_counter.update(_load_csv_counter(path, columns, family="company_id"))

    person_counter = Counter[str]()
    for path, columns in (
        (
            data_root / "sigep_public_servants" / "sigep_public_servants.csv",
            ("numerodeidentificacion",),
        ),
        (
            data_root / "sigep_sensitive_positions" / "sigep_sensitive_positions.csv",
            ("identificacion_funcionario",),
        ),
        (data_root / "siri_antecedents" / "siri_antecedents.csv", ("NUMERO_IDENTIFICACION",)),
        (
            data_root / "cuentas_claras_income_2019" / "cuentas_claras_income_2019.csv",
            ("ing_identificacion", "can_identificacion"),
        ),
    ):
        person_counter.update(_load_csv_counter(path, columns, family="person_id"))

    company_ids = _graph_value_set(
        repo_root,
        label="Company",
        properties=("document_id", "nit"),
        normalizer_family="company_id",
    )
    person_ids = _graph_value_set(
        repo_root,
        label="Person",
        properties=("document_id", "cedula"),
        normalizer_family="person_id",
    )
    company_names = _graph_value_set(
        repo_root,
        label="Company",
        properties=("name", "razon_social"),
        normalizer_family="company_name",
    )
    person_names = _graph_value_set(
        repo_root,
        label="Person",
        properties=("name",),
        normalizer_family="person_name",
    )

    contract_seed = sorted(contract_ids)[:30]
    process_seed = sorted(process_ids)[:30]
    company_seed = [value for value, _ in company_counter.most_common(60)]
    person_seed = [value for value, _ in person_counter.most_common(60)]
    bpin_seed = sorted(bpin_codes)[:30]

    return UniverseIndexes(
        contract_ids=contract_ids,
        process_ids=process_ids,
        company_ids=company_ids,
        person_ids=person_ids,
        bpin_codes=bpin_codes,
        company_names=company_names,
        person_names=person_names,
        contract_seed=contract_seed,
        process_seed=process_seed,
        company_seed=company_seed,
        person_seed=person_seed,
        bpin_seed=bpin_seed,
    )


class SocrataProbeClient:
    def __init__(self, *, domain: str = "www.datos.gov.co", timeout: int = 30) -> None:
        self.domain = domain
        self.client = httpx.Client(timeout=timeout, follow_redirects=True)

    def close(self) -> None:
        self.client.close()

    def view_metadata(self, dataset_id: str) -> dict[str, Any]:
        response = self.client.get(f"https://{self.domain}/api/views/{dataset_id}.json")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected metadata payload for {dataset_id}")
        return payload

    def count_rows(self, dataset_id: str) -> int | None:
        response = self.client.get(
            f"https://{self.domain}/resource/{dataset_id}.json",
            params={"$select": "count(*)"},
        )
        response.raise_for_status()
        payload = response.json()
        if not payload:
            return 0
        count_value = clean_text(payload[0].get("count"))
        return int(count_value) if count_value else None

    def sample_rows(self, dataset_id: str, *, limit: int = 250) -> list[dict[str, Any]]:
        response = self.client.get(
            f"https://{self.domain}/resource/{dataset_id}.json",
            params={"$limit": limit},
        )
        response.raise_for_status()
        payload = response.json()
        return [row for row in payload if isinstance(row, dict)]

    def probe_connected_count(
        self,
        dataset_id: str,
        *,
        column: str,
        values: list[str],
        family: str,
        max_clause_chars: int = 11_000,
    ) -> int:
        if not values:
            return 0

        normalized_values = [value for value in values if clean_text(value)]
        if family in {"company_id", "person_id", "bpin"}:
            normalized_values = [
                strip_document(value)
                for value in normalized_values
                if strip_document(value)
            ]
        clauses = build_in_clauses(
            column=column,
            values=tuple(normalized_values),
            max_clause_chars=max_clause_chars,
        )
        total = 0
        for where_clause in clauses:
            response = self.client.get(
                f"https://{self.domain}/resource/{dataset_id}.json",
                params={"$select": "count(*)", "$where": where_clause},
            )
            response.raise_for_status()
            payload = response.json()
            if payload and clean_text(payload[0].get("count")):
                total += int(payload[0]["count"])
            time.sleep(0.1)
        return total


def _probe_sample_overlap(
    *,
    family: str,
    actual_column: str,
    sample_rows: list[dict[str, Any]],
    universe: UniverseIndexes,
) -> tuple[int, int]:
    normalized_values = {
        _normalize_exact_value(row.get(actual_column), family)
        for row in sample_rows
    }
    normalized_values.discard("")
    sample_overlap = len(normalized_values & universe.full_set_for_family(family))
    return len(normalized_values), sample_overlap


def classify_probe_result(result: DatasetProbeResult) -> tuple[str, str]:
    strong_hits: list[str] = []
    exact_hits = 0
    weak_hits = 0

    for family in FAMILY_PRIORITY:
        for probe in result.probes.get(family, []):
            exact_hits += probe.sample_overlap
            if probe.live_probe_hits:
                strong_hits.append(f"{family}:{probe.actual_name}:{probe.live_probe_hits}")

    for family in WEAK_NAME_FAMILIES:
        weak_hits += sum(probe.sample_overlap for probe in result.probes.get(family, []))

    if strong_hits:
        return (
            "implement",
            "Connected live SoQL probes hit the current graph universe: "
            + ", ".join(strong_hits[:4]),
        )
    if exact_hits >= 5:
        return (
            "enrichment_only",
            "Sample rows already overlap current normalized exact-id universes "
            f"({exact_hits} exact sample matches).",
        )
    if weak_hits > 0 or result.key_families.get("territory"):
        return (
            "weak_feeder",
            "Only weak name or territory overlap was observed; not enough for direct promotion.",
        )
    if result.errors:
        return ("drop", "Dataset could not be probed cleanly from the official endpoint.")
    return ("drop", "No meaningful exact-id overlap with the current graph universe was detected.")


def probe_dataset(
    client: SocrataProbeClient,
    *,
    dataset_id: str,
    universe: UniverseIndexes,
    sample_limit: int = 250,
) -> DatasetProbeResult:
    errors: list[str] = []
    try:
        metadata = client.view_metadata(dataset_id)
    except Exception as exc:  # noqa: BLE001
        return DatasetProbeResult(
            dataset_id=dataset_id,
            title="",
            description="",
            row_count=None,
            updated_at=None,
            columns=[],
            key_families={},
            probes={},
            recommendation="drop",
            reason="Dataset metadata unavailable from the official endpoint.",
            errors=[str(exc)],
        )

    columns = [clean_text(column.get("fieldName")) for column in metadata.get("columns", [])]
    columns = [column for column in columns if column]
    families = infer_key_families(columns)

    row_count: int | None = None
    try:
        row_count = client.count_rows(dataset_id)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"count failed: {exc}")

    sample_rows: list[dict[str, Any]] = []
    try:
        sample_rows = client.sample_rows(dataset_id, limit=sample_limit)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"sample failed: {exc}")

    probes: dict[str, list[ColumnProbe]] = {}
    for family, actual_columns in families.items():
        family_probes: list[ColumnProbe] = []
        for actual_column in actual_columns[:4]:
            non_empty_sample_values, sample_overlap = _probe_sample_overlap(
                family=family,
                actual_column=actual_column,
                sample_rows=sample_rows,
                universe=universe,
            )
            live_probe_hits: int | None = None
            if family in EXACT_FAMILIES:
                try:
                    live_probe_hits = client.probe_connected_count(
                        dataset_id,
                        column=actual_column,
                        values=universe.seed_values_for_family(family),
                        family=family,
                    )
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{family}:{actual_column} live probe failed: {exc}")
            family_probes.append(
                ColumnProbe(
                    actual_name=actual_column,
                    normalized_name=normalize_column_name(actual_column),
                    non_empty_sample_values=non_empty_sample_values,
                    sample_overlap=sample_overlap,
                    live_probe_hits=live_probe_hits,
                )
            )
        probes[family] = family_probes

    result = DatasetProbeResult(
        dataset_id=dataset_id,
        title=clean_text(metadata.get("name")),
        description=clean_text(metadata.get("description")),
        row_count=row_count,
        updated_at=clean_text(metadata.get("rowsUpdatedAt"))
        or clean_text(metadata.get("viewLastModified")),
        columns=columns,
        key_families=families,
        probes=probes,
        recommendation="drop",
        reason="",
        errors=errors,
    )
    result.recommendation, result.reason = classify_probe_result(result)
    return result


def write_probe_outputs(
    *,
    results: list[DatasetProbeResult],
    json_path: Path,
    markdown_path: Path,
) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)

    payload = [asdict(result) for result in results]
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    grouped: dict[str, list[DatasetProbeResult]] = defaultdict(list)
    for result in results:
        grouped[result.recommendation].append(result)

    sections = [
        ("implement", "Implement Next"),
        ("enrichment_only", "Enrichment Only"),
        ("weak_feeder", "Weak Feeder"),
        ("drop", "Drop"),
    ]

    lines = [
        "# Colombia candidate dataset probe",
        "",
        "This report was generated from official `datos.gov.co` metadata and sample rows, "
        "then probed against the current live graph universe using normalized contract, "
        "process, company, person, and BPIN keys.",
        "",
    ]
    for key, title in sections:
        lines.append(f"## {title}")
        lines.append("")
        entries = grouped.get(key, [])
        if not entries:
            lines.append("_None._")
            lines.append("")
            continue
        for result in entries:
            lines.append(f"### {result.dataset_id} - {result.title or 'Unknown dataset'}")
            lines.append("")
            lines.append(f"- Rows: `{result.row_count}`")
            lines.append(f"- Families: `{', '.join(sorted(result.key_families)) or 'none'}`")
            lines.append(f"- Reason: {result.reason}")
            if result.errors:
                lines.append(f"- Errors: `{' | '.join(result.errors[:3])}`")
            for family, probes in sorted(result.probes.items()):
                probe_bits = []
                for probe in probes:
                    bit = (
                        f"{probe.actual_name}"
                        f" sample={probe.sample_overlap}/{probe.non_empty_sample_values}"
                    )
                    if probe.live_probe_hits is not None:
                        bit += f" live={probe.live_probe_hits}"
                    probe_bits.append(bit)
                lines.append(f"- {family}: `{'; '.join(probe_bits)}`")
            lines.append("")

    markdown_path.write_text("\n".join(lines), encoding="utf-8")
