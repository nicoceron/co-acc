from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from coacc_etl.lakehouse.paths import lake_root
from coacc_etl.models.narrator.prompt import build_prompt
from coacc_etl.models.narrator.subgraph import CaseSubgraph, NarratorError, extract
from coacc_etl.models.narrator.verify import NarrativeVerificationResult, check
from coacc_etl.qualification.llm_review import (
    DEFAULT_LLM_MODELS,
    _call_anthropic,
    _call_gemini,
    _call_openai,
)

if TYPE_CHECKING:
    from pathlib import Path

_SAFE_CASE_ID = re.compile(r"[^A-Za-z0-9_.=-]+")


@dataclass(frozen=True)
class NarrativeGenerationResult:
    case_id: str
    narrative_path: str
    provider: str
    valid: bool
    word_count: int
    errors: list[str]


def _citation_text(subgraph: CaseSubgraph, index: int = 0) -> str:
    if not subgraph.evidence:
        raise NarratorError("subgraph has no evidence citations")
    citation = subgraph.evidence[min(index, len(subgraph.evidence) - 1)]
    return f"({citation.dataset_id}, {citation.row_key})"


def build_templated_narrative(subgraph: CaseSubgraph) -> str:
    primary_citation = _citation_text(subgraph, 0)
    secondary_citation = (
        _citation_text(subgraph, 1) if len(subgraph.evidence) > 1 else primary_citation
    )
    supplier = subgraph.supplier_name or "el proveedor registrado"
    buyer = subgraph.buyer_name or "la entidad compradora registrada"
    modality = subgraph.procurement_modality or "modalidad no normalizada"
    features = ", ".join(subgraph.anomaly.top_features) or "variables de comportamiento contractual"
    signal_titles = "; ".join(signal.title for signal in subgraph.signals) or (
        "sin senales publicas materializadas adicionales"
    )
    value_text = (
        f"valor registrado {subgraph.contract_value:,.0f}"
        if subgraph.contract_value is not None
        else "valor contractual no normalizado"
    )
    lead = (
        f"El contrato {subgraph.contract_id} queda priorizado para revision porque el "
        f"modelo de anomalias le asigna una puntuacion de {subgraph.anomaly.score:.3f} "
        f"con confianza {subgraph.anomaly.score_confidence}. La lectura no afirma una "
        "irregularidad: ordena un expediente publico para que un analista revise el "
        f"contexto, la trazabilidad y los soportes. En el recorte disponible aparecen "
        f"{buyer}, {supplier}, la modalidad {modality} y el {value_text}. La primera "
        f"fuente verificable es el registro contractual de SECOP II {primary_citation}."
    )
    evidence = (
        "La evidencia minima conecta el identificador del contrato, el enlace del "
        "proceso y las partes reportadas en la tabla curada de adjudicaciones. Ese "
        "registro permite ubicar el proceso, revisar el objeto contractual y confirmar "
        "que el proveedor y la entidad compradora fueron extraidos desde una fuente "
        f"publica reproducible {primary_citation}. Cuando hay soportes de senales, el "
        "expediente conserva la referencia original y no la reemplaza por texto libre; "
        f"por eso la cita secundaria disponible para este caso es {secondary_citation}. "
        "Esta narrativa debe leerse como resumen de revision y no como conclusion "
        "juridica."
    )
    signals = (
        "El puntaje proviene de un Isolation Forest entrenado sobre contratos SECOP "
        f"curados. Los principales impulsores registrados para este caso son {features}. "
        f"Las senales materializadas asociadas son: {signal_titles}. Si una senal "
        "menciona sanciones o hallazgos, el texto solo indica coincidencia documental "
        "y exige revisar la fuente original antes de publicar cualquier afirmacion "
        "externa. La combinacion de puntaje, soporte y trazabilidad ayuda a ordenar "
        "trabajo analitico, especialmente cuando el proveedor tiene poco historial o "
        "cuando el valor se aparta de pares comparables."
    )
    sources = (
        f"Fuentes citadas: contrato SECOP II {primary_citation}; soporte complementario "
        f"{secondary_citation}. El expediente debe conservar estos identificadores en "
        "cualquier publicacion, porque cada cita corresponde a una fila o selector "
        "verificable del lago. Si se actualiza el lago, la narrativa debe regenerarse "
        "con el mismo verificador para comprobar secciones, longitud, entidades "
        "mencionadas y citas."
    )
    return (
        f"# Lead\n{lead}\n\n"
        f"## Evidencia\n{evidence}\n\n"
        f"## Señales\n{signals}\n\n"
        f"## Fuentes\n{sources}"
    )


def _provider_key(provider: str, api_key: str | None) -> str:
    if api_key:
        return api_key
    if provider == "anthropic":
        return os.environ.get("ANTHROPIC_API_KEY", "")
    if provider == "openai":
        return os.environ.get("OPENAI_API_KEY", "")
    if provider == "gemini":
        return (
            os.environ.get("GEMINI_API_KEY", "")
            or os.environ.get("GOOGLE_API_KEY", "")
            or os.environ.get("GOOGLE_GENERATIVE_AI_API_KEY", "")
        )
    return ""


def call_llm(
    prompt: str,
    *,
    provider: str,
    model: str | None = None,
    api_key: str | None = None,
    max_tokens: int = 600,
) -> str:
    normalized = provider.strip().lower()
    selected_model = model or DEFAULT_LLM_MODELS.get(normalized, "")
    key = _provider_key(normalized, api_key)
    if not selected_model:
        raise NarratorError(f"unknown narrator provider: {provider}")
    if not key:
        raise NarratorError(f"{normalized} API key not set")
    if normalized == "anthropic":
        return _call_anthropic(prompt, model=selected_model, api_key=key, max_tokens=max_tokens)
    if normalized == "openai":
        return _call_openai(prompt, model=selected_model, api_key=key, max_tokens=max_tokens)
    if normalized == "gemini":
        return _call_gemini(prompt, model=selected_model, api_key=key, max_tokens=max_tokens)
    raise NarratorError(f"unknown narrator provider: {provider}")


def _narrative_path(case_id: str, output: Path | None = None) -> Path:
    if output is not None:
        return output
    safe = _SAFE_CASE_ID.sub("_", case_id.strip()).strip("._") or "case"
    return lake_root() / "curated" / "narratives" / f"{safe}.md"


def _write_narrative(path: Path, narrative: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(narrative, encoding="utf-8")


def generate_narrative(
    case_id: str,
    *,
    provider: str = "gemini",
    model: str | None = None,
    api_key: str | None = None,
    require_llm: bool = False,
    output: Path | None = None,
) -> NarrativeGenerationResult:
    subgraph = extract(case_id)
    selected_provider = provider.strip().lower()
    narrative = ""
    verification: NarrativeVerificationResult | None = None
    used_provider = "template"
    feedback: list[str] = []

    if selected_provider != "template":
        has_key = bool(_provider_key(selected_provider, api_key))
        if not has_key and require_llm:
            raise NarratorError(f"{selected_provider} API key not set")
        if has_key:
            for _ in range(3):
                prompt = build_prompt(subgraph, verifier_feedback=feedback or None)
                candidate = call_llm(
                    prompt,
                    provider=selected_provider,
                    model=model,
                    api_key=api_key,
                    max_tokens=600,
                )
                verification = check(candidate, subgraph)
                if verification.valid:
                    narrative = candidate
                    used_provider = selected_provider
                    break
                feedback = verification.errors

    if not narrative:
        narrative = build_templated_narrative(subgraph)
        verification = check(narrative, subgraph)
        if not verification.valid:
            error_text = "; ".join(verification.errors)
            raise NarratorError(f"templated narrative failed verification: {error_text}")

    assert verification is not None
    path = _narrative_path(case_id, output=output)
    _write_narrative(path, narrative)
    return NarrativeGenerationResult(
        case_id=case_id,
        narrative_path=str(path),
        provider=used_provider,
        valid=verification.valid,
        word_count=verification.word_count,
        errors=verification.errors,
    )
