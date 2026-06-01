from __future__ import annotations

from typing import TYPE_CHECKING

import yaml  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from coacc_etl.models.narrator.subgraph import CaseSubgraph


def build_prompt(subgraph: CaseSubgraph, *, verifier_feedback: list[str] | None = None) -> str:
    payload = yaml.safe_dump(
        subgraph.to_dict(),
        allow_unicode=True,
        sort_keys=True,
    )
    feedback = ""
    if verifier_feedback:
        feedback = "\nVerifier feedback to fix:\n" + "\n".join(
            f"- {item}" for item in verifier_feedback
        )
    return (
        "Escribe una narrativa analitica en espanol para revision publica.\n"
        "Usa exactamente estas secciones: # Lead, ## Evidencia, ## Señales, ## Fuentes.\n"
        "Longitud obligatoria: 250 a 400 palabras.\n"
        "No afirmes delito, culpabilidad ni corrupcion. Usa lenguaje de patrones y priorizacion.\n"
        "Menciona solo entidades presentes en nodes.\n"
        "Cada afirmacion factual debe tener una cita con formato (dataset_id, row_key), y la cita "
        "debe venir de evidence.\n"
        "Devuelve solo Markdown.\n\n"
        "Subgraph YAML:\n"
        f"{payload}"
        f"{feedback}"
    )
