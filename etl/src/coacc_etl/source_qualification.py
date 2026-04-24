#!/usr/bin/env python3
# ruff: noqa: E501
"""Pre-lake source qualification gate.

Reads the current source inventories, probes Socrata metadata + column schemas,
then proves join keys against the current registry's known key classes. The
Gemini LLM pass is a required second-review gate for government datasets with
badly named columns.

  1. docs/datasets/source_qualification_catalog.csv
     - Full metadata/rowcount/column profile for every probed dataset
  2. docs/datasets/source_qualification_report.md
     - Human-readable report grouped by recommendation
  3. docs/datasets/source_qualification_proven.csv
     - Only datasets where at least one join key was proven

Architecture
~~~~~~~~~~~~

  source inventories (appendix, audit JSON, registry, env-configured pipelines)
         |
         v  (filter: recommendation in candidates + valid audit_status)
  Socrata metadata API  ──►  column schema + rowcount per dataset
         |
         v  (column name normalization + join-key matching)
  source_qualification_catalog.csv  ──►  deterministic + LLM review
         |
         v  (filter: proven join keys only)
  source_qualification_proven.csv  ──►  feed into main ETL source contracts

Join key classes (heavily normalized):
  - nit:        document_id, numero_documento, nit, documento_proveedor,
                numero_identificacion, id_contratista, num_documento
  - contract:   id_contrato, numero_contrato, contrato_id, cod_contrato,
                reference_contract_number
  - process:    id_proceso, numero_proceso, proceso_id, cod_proceso
  - bpin:       bpin, codigo_bpin, numero_bpin
  - divipola:   cod_municipio, cod_departamento, codigo_divipola,
                cod_dpto, cod_mpio, divipola
  - entity:     nit_entidad, cod_entidad, id_entidad, codigo_entidad

Normalization: strip accents, lowercase, collapse whitespace, remove
non-alphanumeric chars, so "N\xfamero Documento" and "numero_documento"
both match.

Usage:
    python -m coacc_etl.source_qualification
        [--appendix docs/datasets/dataset_relevance_appendix.csv]
        [--catalog-out docs/datasets/source_qualification_catalog.csv]
        [--proven-out docs/datasets/source_qualification_proven.csv]
        [--report-out docs/datasets/source_qualification_report.md]
        [--limit N]               # probe only first N candidates (smoke)
        [--sleep 0.25]            # seconds between Socrata API calls
        [--probe-sample 5]        # sample N rows to check join-key density
        [--include-current]       # also probe current_registry sources
        [--all-known]             # audit JSON + appendix + registry + env IDs
        [--metadata-only]         # no count query, no sample rows
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

LOG = logging.getLogger("source_qualification")
REPO_ROOT = Path(__file__).resolve().parents[3]

SOCRATA_ID_RE = re.compile(r"(?<![a-z0-9])([a-z0-9]{4}-[a-z0-9]{4})(?![a-z0-9])")

DEFAULT_LLM_MODELS = {
    "anthropic": "claude-sonnet-4-20250514",
    "gemini": "gemini-2.5-flash-lite",
    "openai": "gpt-4o-mini",
}

LLM_JOIN_KEY_PROMPT = """\
You are a Colombian open-data expert analyzing dataset schemas for join-key discovery.

Dataset: {name} ({rows} rows)
Sector: {sector}
Columns ({n_cols}):
{columns_blob}

Known join-key classes and their purpose:
- nit: NIT, document ID, or taxpayer ID linking to people/companies
  (e.g. nit, documento_proveedor, identificacion_del_contratista, numero_nit)
- contract: Contract identifier linking to SECOP contracts
  (e.g. id_contrato, numero_contrato, identificadorcontrato, referencia_contrato)
- process: Procurement process identifier
  (e.g. id_proceso, numero_proceso, id_del_proceso_de_compra, id_procedimiento)
- bpin: BPIN project code linking to DNP investment projects
  (e.g. codigo_bpin, bpin, codigobpin)
- divipola: DANE geographic code (department or municipality code)
  (e.g. codigo_municipio, cod_dane_departamento, cod_dpto, divipola, codmun)
- entity: Entity/institution code linking to supervised entities
  (e.g. codigo_entidad, cod_entidad, c_digo_entidad, id_entidad)

For EACH column that could map to one of these classes, output a JSON object.
Consider variants: camelCase (identificadorcontrato→contract), suffixes (_compradora, _proveedor, _grupo),
abbreviations (c_digo→codigo, num→numero), prefixes (codigo→code).

Strict rule: only include stable identifiers or administrative codes. Do NOT
include names, descriptions, free text, dates, addresses, locations without a
code, categories, values, or fuzzy-match fields such as nombre_entidad,
nombre_proveedor, razon_social, municipio, departamento, objeto, descripcion,
or direccion. A badly named column can qualify only if the name still implies
an identifier/code/document number/contract/process/BPIN/DIVIPOLA/entity code.

Only include columns where you have high confidence (>=0.75).
If no columns map, return an empty array.

Output ONLY a JSON array, no other text:
```json
[{{"column": "original_column_name", "normalized": "normalized_form", "join_class": "nit|contract|process|bpin|divipola|entity", "confidence": 0.0-1.0, "reasoning": "brief reason"}}]
```"""

LLM_RELEVANCE_PROMPT = """\
You are a corruption-detection expert assessing Colombian open-data datasets.

Dataset: {name} ({rows} rows)
Sector: {sector}
Proven join keys: {join_keys}
Column sample: {column_sample}
Recommendation from triage: {recommendation}

Assess this dataset's relevance for detecting corruption, fraud, or irregularities
in Colombian public procurement and governance. Consider:
- Procurement/contract data (bidding, modifications, execution, sanctions)
- Financial data (budget, execution, payments, invoices, royalties)
- Entity/identity registries (supplier directories, entity directories)
- Audit trail data (modifications, complaints, sanctions)
- Project investment data (BPIN-linked, DNP, royalties)
- Supervised entity reports (SFC, financial sector)

Output ONLY a JSON object, no other text:
```json
{{"relevance_tier": "ingest_priority|ingest|ingest_if_useful|context_enrichment|context_if_useful|skip", "reasoning": "1-2 sentence explanation", "corruption_signals": ["signal1", ...]}}
```"""


def _try_llm_review(
    rows: list[TriageCatalogRow],
    *,
    provider: str = "anthropic",
    model: str = "",
    api_key: str | None = None,
    batch_size: int = 5,
    sleep_between: float = 1.0,
    cache_path: Path | None = None,
) -> dict[str, dict[str, Any]]:
    model = model or DEFAULT_LLM_MODELS.get(provider, "")
    if provider == "anthropic":
        return _llm_review_anthropic(
            rows, model=model, api_key=api_key,
            batch_size=batch_size, sleep_between=sleep_between,
            cache_path=cache_path,
        )
    if provider == "openai":
        return _llm_review_openai(
            rows, model=model, api_key=api_key,
            batch_size=batch_size, sleep_between=sleep_between,
            cache_path=cache_path,
        )
    if provider == "gemini":
        return _llm_review_gemini(
            rows, model=model, api_key=api_key,
            batch_size=batch_size, sleep_between=sleep_between,
            cache_path=cache_path,
        )
    LOG.warning("unknown LLM provider: %s, skipping review", provider)
    return {}


def _load_cache(cache_path: Path | None) -> dict[str, dict[str, Any]]:
    if cache_path and cache_path.exists():
        with cache_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_cache(cache: dict[str, dict[str, Any]], cache_path: Path | None) -> None:
    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)


def _call_anthropic(prompt: str, *, model: str, api_key: str, max_tokens: int = 2048) -> str:
    try:
        import anthropic
    except ImportError:
        LOG.error("anthropic package not installed. Run: pip install anthropic")
        raise
    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


def _call_openai(prompt: str, *, model: str, api_key: str, max_tokens: int = 2048) -> str:
    try:
        from openai import OpenAI
    except ImportError:
        LOG.error("openai package not installed. Run: pip install openai")
        raise
    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content


def _call_gemini(
    prompt: str,
    *,
    model: str,
    api_key: str,
    max_tokens: int = 2048,
    max_attempts: int = 4,
) -> str:
    """Call Gemini REST generateContent without adding another SDK dependency."""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    backoff = 2.0
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = httpx.post(
                url,
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": api_key,
                },
                json={
                    "contents": [
                        {
                            "role": "user",
                            "parts": [{"text": prompt}],
                        }
                    ],
                    "generationConfig": {
                        "temperature": 0,
                        "maxOutputTokens": max_tokens,
                    },
                },
                timeout=60.0,
            )
            if response.status_code in {429, 500, 502, 503, 504}:
                raise httpx.HTTPStatusError(
                    f"retryable {response.status_code}",
                    request=response.request,
                    response=response,
                )
            break
        except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
            last_error = exc
            if attempt >= max_attempts:
                raise
            LOG.warning("Gemini call retry %d/%d after %s", attempt, max_attempts, exc)
            time.sleep(backoff)
            backoff *= 2
    else:
        raise RuntimeError(f"Gemini call exhausted retries: {last_error}") from last_error

    response.raise_for_status()
    payload = response.json()
    candidates = payload.get("candidates") or []
    if not candidates:
        return ""
    content = candidates[0].get("content") or {}
    parts = content.get("parts") or []
    return "\n".join(
        str(part.get("text", ""))
        for part in parts
        if isinstance(part, dict) and part.get("text")
    )


def _parse_json_from_response(text: str) -> Any:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        start = next((i for i, line in enumerate(lines) if line.strip().startswith("[")), 0)
        end = next(
            (i for i, line in enumerate(lines) if line.strip() == "```" and i > start),
            len(lines),
        )
        text = "\n".join(lines[start:end])
    text = text.strip().removeprefix("```json").removesuffix("```").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        LOG.warning("failed to parse LLM JSON response: %s...", text[:200])
        return None


def _llm_review_anthropic(
    rows: list[TriageCatalogRow],
    *,
    model: str,
    api_key: str | None,
    batch_size: int,
    sleep_between: float,
    cache_path: Path | None,
) -> dict[str, dict[str, Any]]:
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        LOG.warning("ANTHROPIC_API_KEY not set, skipping LLM review")
        return {}
    cache = _load_cache(cache_path)
    results: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row.dataset_id in cache:
            results[row.dataset_id] = cache[row.dataset_id]
            continue
        col_list = row.columns_all.split("|") if row.columns_all else []
        if len(col_list) < 2:
            continue
        columns_blob = "\n".join(f"  {i+1}. {c}" for i, c in enumerate(col_list[:80]))
        join_key_prompt = LLM_JOIN_KEY_PROMPT.format(
            name=row.name, rows=row.rows, sector=row.sector,
            n_cols=len(col_list), columns_blob=columns_blob,
        )
        try:
            raw = _call_anthropic(join_key_prompt, model=model, api_key=key)
            parsed = _parse_json_from_response(raw)
        except Exception as exc:
            LOG.warning("LLM join-key review failed for %s: %s", row.dataset_id, exc)
            parsed = None
        join_key_findings = parsed if isinstance(parsed, list) else []
        column_sample = ", ".join(col_list[:15])
        relevance_prompt = LLM_RELEVANCE_PROMPT.format(
            name=row.name, rows=row.rows, sector=row.sector,
            join_keys=row.join_key_classes or "none",
            column_sample=column_sample,
            recommendation=classify_dataset(row),
        )
        try:
            raw = _call_anthropic(relevance_prompt, model=model, api_key=key)
            parsed = _parse_json_from_response(raw)
        except Exception as exc:
            LOG.warning("LLM relevance review failed for %s: %s", row.dataset_id, exc)
            parsed = None
        relevance = parsed if isinstance(parsed, dict) else {}
        entry = {
            "join_key_findings": join_key_findings,
            "relevance": relevance,
        }
        results[row.dataset_id] = entry
        cache[row.dataset_id] = entry
        _save_cache(cache, cache_path)
        LOG.info(
            "LLM reviewed %s: found %d keys, tier=%s",
            row.dataset_id,
            len(join_key_findings),
            relevance.get("relevance_tier", "?"),
        )
        if sleep_between > 0:
            time.sleep(sleep_between)
    return results


def _llm_review_openai(
    rows: list[TriageCatalogRow],
    *,
    model: str,
    api_key: str | None,
    batch_size: int,
    sleep_between: float,
    cache_path: Path | None,
) -> dict[str, dict[str, Any]]:
    key = api_key or os.environ.get("OPENAI_API_KEY", "")
    if not key:
        LOG.warning("OPENAI_API_KEY not set, skipping LLM review")
        return {}
    cache = _load_cache(cache_path)
    results: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row.dataset_id in cache:
            results[row.dataset_id] = cache[row.dataset_id]
            continue
        col_list = row.columns_all.split("|") if row.columns_all else []
        if len(col_list) < 2:
            continue
        columns_blob = "\n".join(f"  {i+1}. {c}" for i, c in enumerate(col_list[:80]))
        join_key_prompt = LLM_JOIN_KEY_PROMPT.format(
            name=row.name, rows=row.rows, sector=row.sector,
            n_cols=len(col_list), columns_blob=columns_blob,
        )
        try:
            raw = _call_openai(join_key_prompt, model=model, api_key=key)
            parsed = _parse_json_from_response(raw)
        except Exception as exc:
            LOG.warning("LLM join-key review failed for %s: %s", row.dataset_id, exc)
            parsed = None
        join_key_findings = parsed if isinstance(parsed, list) else []
        column_sample = ", ".join(col_list[:15])
        relevance_prompt = LLM_RELEVANCE_PROMPT.format(
            name=row.name, rows=row.rows, sector=row.sector,
            join_keys=row.join_key_classes or "none",
            column_sample=column_sample,
            recommendation=classify_dataset(row),
        )
        try:
            raw = _call_openai(relevance_prompt, model=model, api_key=key)
            parsed = _parse_json_from_response(raw)
        except Exception as exc:
            LOG.warning("LLM relevance review failed for %s: %s", row.dataset_id, exc)
            parsed = None
        relevance = parsed if isinstance(parsed, dict) else {}
        entry = {
            "join_key_findings": join_key_findings,
            "relevance": relevance,
        }
        results[row.dataset_id] = entry
        cache[row.dataset_id] = entry
        _save_cache(cache, cache_path)
        LOG.info(
            "LLM reviewed %s: found %d keys, tier=%s",
            row.dataset_id,
            len(join_key_findings),
            relevance.get("relevance_tier", "?"),
        )
        if sleep_between > 0:
            time.sleep(sleep_between)
    return results


def _llm_review_gemini(
    rows: list[TriageCatalogRow],
    *,
    model: str,
    api_key: str | None,
    batch_size: int,
    sleep_between: float,
    cache_path: Path | None,
) -> dict[str, dict[str, Any]]:
    key = (
        api_key
        or os.environ.get("GEMINI_API_KEY", "")
        or os.environ.get("GOOGLE_API_KEY", "")
        or os.environ.get("GOOGLE_GENERATIVE_AI_API_KEY", "")
    )
    if not key:
        LOG.warning("GEMINI_API_KEY/GOOGLE_API_KEY not set, skipping LLM review")
        return {}
    cache = _load_cache(cache_path)
    results: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row.dataset_id in cache:
            results[row.dataset_id] = cache[row.dataset_id]
            continue
        col_list = row.columns_all.split("|") if row.columns_all else []
        if len(col_list) < 2:
            continue
        columns_blob = "\n".join(f"  {i+1}. {c}" for i, c in enumerate(col_list[:80]))
        join_key_prompt = LLM_JOIN_KEY_PROMPT.format(
            name=row.name, rows=row.rows, sector=row.sector,
            n_cols=len(col_list), columns_blob=columns_blob,
        )
        try:
            raw = _call_gemini(join_key_prompt, model=model, api_key=key)
            parsed = _parse_json_from_response(raw)
        except Exception as exc:
            LOG.warning("LLM join-key review failed for %s: %s", row.dataset_id, exc)
            parsed = None
        join_key_findings = parsed if isinstance(parsed, list) else []
        column_sample = ", ".join(col_list[:15])
        relevance_prompt = LLM_RELEVANCE_PROMPT.format(
            name=row.name, rows=row.rows, sector=row.sector,
            join_keys=row.join_key_classes or "none",
            column_sample=column_sample,
            recommendation=classify_dataset(row),
        )
        try:
            raw = _call_gemini(relevance_prompt, model=model, api_key=key)
            parsed = _parse_json_from_response(raw)
        except Exception as exc:
            LOG.warning("LLM relevance review failed for %s: %s", row.dataset_id, exc)
            parsed = None
        relevance = parsed if isinstance(parsed, dict) else {}
        entry = {
            "join_key_findings": join_key_findings,
            "relevance": relevance,
        }
        results[row.dataset_id] = entry
        cache[row.dataset_id] = entry
        _save_cache(cache, cache_path)
        LOG.info(
            "LLM reviewed %s: found %d keys, tier=%s",
            row.dataset_id,
            len(join_key_findings),
            relevance.get("relevance_tier", "?"),
        )
        if sleep_between > 0:
            time.sleep(sleep_between)
    return results


def _apply_llm_findings(
    rows: list[TriageCatalogRow],
    llm_results: dict[str, dict[str, Any]],
    *,
    client: httpx.Client,
    domain: str,
    probe_sample: int,
    min_confidence: float,
) -> list[TriageCatalogRow]:
    updated: list[TriageCatalogRow] = []
    for row in rows:
        result = llm_results.get(row.dataset_id)
        if not result:
            updated.append(row)
            continue
        known_norm_cols = {
            _normalize_col(col)
            for col in row.columns_all.split("|")
            if col
        }
        findings = result.get("join_key_findings", [])
        if findings:
            new_cols: list[str] = []
            for f in findings:
                col = f.get("column", "")
                key_class = f.get("join_class", "")
                try:
                    confidence = float(f.get("confidence", 0))
                except (TypeError, ValueError):
                    confidence = 0.0
                norm = _normalize_col(col)
                if not col or not key_class or confidence < min_confidence:
                    continue
                if norm not in known_norm_cols:
                    continue
                if not _is_valid_llm_join_key(norm, key_class):
                    continue
                if key_class not in JOIN_KEY_CLASSES:
                    continue
                existing = row.join_key_columns or ""
                spec = f"{key_class}:{norm}"
                if spec in existing:
                    continue
                new_cols.append(spec)
            if new_cols:
                all_specs = list(row.join_key_columns.split("|")) if row.join_key_columns else []
                all_specs.extend(new_cols)
                row.join_key_columns = "|".join(all_specs)
                all_classes = set(row.join_key_classes.split("|")) if row.join_key_classes else set()
                old_classes = set(all_classes)
                for spec in new_cols:
                    cls_name = spec.split(":")[0]
                    all_classes.add(cls_name)
                row.join_key_classes = "|".join(sorted(all_classes))
                row.join_keys_found = len(all_classes)
                added_classes = len(all_classes - old_classes)
                row.probe_notes.append(
                    f"llm_added_columns={len(new_cols)},classes={added_classes}"
                )
                if probe_sample > 0 and row.rows > 0:
                    _probe_join_density(
                        client, row, domain=domain, sample_size=probe_sample,
                    )
        relevance = result.get("relevance", {})
        llm_tier = relevance.get("relevance_tier", "")
        row.probe_notes.append(f"llm_tier={llm_tier}")
        reasoning = relevance.get("reasoning", "")
        if reasoning:
            row.probe_notes.append(f"llm_reasoning={reasoning[:200]}")
        updated.append(row)
    return updated

DEFAULT_DOMAIN = "www.datos.gov.co"

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


def _build_client(timeout: float) -> httpx.Client:
    headers: dict[str, str] = {"User-Agent": "coacc-joinkey-triage/1.0"}
    app_token = os.environ.get("SOCRATA_APP_TOKEN", "").strip()
    key_id = os.environ.get("SOCRATA_KEY_ID", "").strip()
    key_secret = os.environ.get("SOCRATA_KEY_SECRET", "").strip()

    if app_token and app_token != key_id:
        headers["X-App-Token"] = app_token

    auth = None
    if key_id and key_secret:
        auth = (key_id, key_secret)

    return httpx.Client(
        timeout=timeout,
        headers=headers,
        follow_redirects=True,
        auth=auth,
    )


def _load_env_file(path: Path, *, override: bool = False) -> None:
    """Load simple KEY=VALUE pairs from a local env file without extra deps."""
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        if not override and key in os.environ:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ[key] = value


def _get_with_retry(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, str | int] | None = None,
    max_attempts: int = 5,
    initial_backoff: float = 1.0,
    max_backoff: float = 60.0,
) -> httpx.Response:
    backoff = initial_backoff
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.get(url, params=params)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise httpx.HTTPStatusError(
                    f"retryable {response.status_code}",
                    request=response.request,
                    response=response,
                )
            return response
        except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            LOG.debug("retry %s attempt=%s/%s (%s)", url, attempt, max_attempts, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
    raise RuntimeError(f"exhausted retries for {url}: {last_error}") from last_error


def _parse_socrata_ts(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        try:
            return datetime.fromtimestamp(int(value), tz=UTC)
        except (OSError, ValueError, OverflowError):
            return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.isdigit():
            try:
                return datetime.fromtimestamp(int(s), tz=UTC)
            except (OSError, ValueError, OverflowError):
                return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    return None


def _meaningful_column_count(columns: list[dict[str, object]]) -> int:
    count = 0
    meaningful_re = re.compile(r"^(?=.*[a-z])[a-z0-9_]{3,}$")
    for col in columns:
        field_name = str(col.get("fieldName", "")).lower()
        if meaningful_re.match(field_name):
            count += 1
    return count


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


def probe_dataset(
    client: httpx.Client,
    entry: dict[str, str],
    *,
    domain: str,
    probe_sample: int = 5,
    skip_count: bool = False,
    count_client: httpx.Client | None = None,
) -> TriageCatalogRow:
    row = TriageCatalogRow(
        dataset_id=entry.get("dataset_id", ""),
        name=entry.get("name", ""),
        sector=entry.get("sector_or_category", ""),
        scope=entry.get("scope", ""),
        recommendation=entry.get("recommendation", ""),
        relevance=entry.get("relevance", ""),
        audit_status=entry.get("audit_status", ""),
        source_refs=entry.get("source_refs", ""),
        origin_refs=entry.get("origin_refs", ""),
        signal_refs=entry.get("signal_refs", ""),
        url=entry.get("url", ""),
    )

    if not row.dataset_id.strip():
        row.probe_notes.append("missing dataset_id")
        return row

    meta_url = f"https://{domain}/api/views/{row.dataset_id}.json"
    try:
        meta_resp = _get_with_retry(client, meta_url)
        if meta_resp.status_code == 403:
            row.probe_notes.append("metadata forbidden 403")
            row.audit_status = "forbidden_403"
            return row
        if meta_resp.status_code == 404:
            row.probe_notes.append("metadata not found 404")
            row.audit_status = "dead_404"
            return row
        if meta_resp.status_code != 200:
            row.probe_notes.append(f"metadata http {meta_resp.status_code}")
            return row
        meta = meta_resp.json()
    except Exception as exc:
        row.probe_notes.append(f"metadata fetch failed: {exc}")
        return row

    columns_raw = [c for c in meta.get("columns", []) if isinstance(c, dict)]
    row.n_columns = len(columns_raw)
    row.n_meaningful_columns = _meaningful_column_count(columns_raw)

    field_names = [str(c.get("fieldName", "")) for c in columns_raw if c.get("fieldName")]
    row.columns_all = "|".join(field_names)

    normalized_cols = [_normalize_col(fn) for fn in field_names]
    found_keys, found_classes = _find_join_keys(normalized_cols)
    row.join_keys_found = len(found_keys)
    row.join_key_classes = "|".join(sorted(found_keys)) if found_keys else ""
    join_key_col_list: list[str] = []
    for cls_name in sorted(found_classes):
        for col_name in found_classes[cls_name]:
            join_key_col_list.append(f"{cls_name}:{col_name}")
    row.join_key_columns = "|".join(join_key_col_list) if join_key_col_list else ""

    row.update_freq = str(
        (meta.get("metadata") or {}).get("custom_fields", {}).get("Periodicidad", "") or ""
    )

    rows_updated = _parse_socrata_ts(meta.get("rowsUpdatedAt"))
    if rows_updated is None:
        row.probe_notes.append("rowsUpdatedAt missing or unparseable")
        row.last_update_days = -1
    else:
        row.last_update = rows_updated.date().isoformat()
        row.last_update_days = (datetime.now(tz=UTC).date() - rows_updated.date()).days

    meta_row_count = meta.get("rows")
    if isinstance(meta_row_count, int) and meta_row_count > 0:
        row.rows = meta_row_count
        row.probe_notes.append("rows_from_metadata")

    if not skip_count:
        count_url = f"https://{domain}/resource/{row.dataset_id}.json"
        try:
            effective_client = count_client or client
            count_resp = _get_with_retry(
                effective_client,
                count_url,
                params={"$select": "count(*)"},
                max_attempts=2,
                initial_backoff=1.0,
                max_backoff=10.0,
            )
            if count_resp.status_code != 200:
                row.probe_notes.append(f"count http {count_resp.status_code}")
            else:
                payload = count_resp.json()
                if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
                    row.probe_notes.append(f"count payload shape invalid: {type(payload).__name__}")
                else:
                    count_val = next(iter(payload[0].values()))
                    try:
                        row.rows = int(str(count_val))
                        if "rows_from_metadata" in row.probe_notes:
                            row.probe_notes.remove("rows_from_metadata")
                    except (TypeError, ValueError):
                        row.probe_notes.append(f"count value unparseable: {count_val!r}")
        except Exception as exc:
            row.probe_notes.append(f"count fetch failed: {exc}")

    if found_keys and probe_sample > 0 and row.rows > 0:
        _probe_join_density(client, row, domain=domain, sample_size=probe_sample)

    return row


def _probe_join_density(
    client: httpx.Client,
    row: TriageCatalogRow,
    *,
    domain: str,
    sample_size: int,
) -> None:
    density_parts: list[str] = []
    for key_class_spec in row.join_key_columns.split("|"):
        if not key_class_spec:
            continue
        parts = key_class_spec.split(":")
        if len(parts) != 2:
            continue
        cls_name, col_name = parts
        api_col_name = col_name
        try:
            url = f"https://{domain}/resource/{row.dataset_id}.json"
            params: dict[str, str | int] = {
                "$select": f"{api_col_name}",
                "$limit": sample_size,
            }
            resp = _get_with_retry(client, url, params=params)
            if resp.status_code != 200:
                density_parts.append(f"{cls_name}:http_{resp.status_code}")
                continue
            payload = resp.json()
            if not isinstance(payload, list):
                density_parts.append(f"{cls_name}:not_list")
                continue
            non_empty = sum(
                1
                for r in payload
                if isinstance(r, dict) and str(r.get(api_col_name, "")).strip()
            )
            density_parts.append(f"{cls_name}:{non_empty}/{len(payload)}")
        except Exception as exc:
            density_parts.append(f"{cls_name}:err({exc})")

    row.sample_join_density = "|".join(density_parts) if density_parts else ""


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


def _extract_socrata_ids(*values: object) -> list[str]:
    seen: set[str] = set()
    ids: list[str] = []
    for value in values:
        if value is None:
            continue
        for match in SOCRATA_ID_RE.finditer(str(value).lower()):
            dataset_id = match.group(1)
            if dataset_id not in seen:
                seen.add(dataset_id)
                ids.append(dataset_id)
    return ids


def _split_refs(value: str) -> set[str]:
    return {part for part in value.split("|") if part}


def _join_refs(refs: set[str]) -> str:
    return "|".join(sorted(refs))


def _merge_entry(
    entries_by_id: dict[str, dict[str, str]],
    entry: dict[str, str],
    *,
    origin_ref: str,
    source_ref: str = "",
    signal_ref: str = "",
) -> None:
    dataset_id = entry.get("dataset_id", "").strip().lower()
    if not dataset_id:
        return
    existing = entries_by_id.setdefault(
        dataset_id,
        {
            "dataset_id": dataset_id,
            "name": "",
            "sector_or_category": "",
            "scope": "",
            "recommendation": "",
            "relevance": "",
            "audit_status": "",
            "source_refs": "",
            "origin_refs": "",
            "signal_refs": "",
            "url": f"https://{DEFAULT_DOMAIN}/d/{dataset_id}",
        },
    )

    for key in ("name", "sector_or_category", "scope", "recommendation", "relevance", "url"):
        value = (entry.get(key) or "").strip()
        if value and not existing.get(key):
            existing[key] = value

    audit_status = (entry.get("audit_status") or "").strip()
    if audit_status and not existing.get("audit_status"):
        existing["audit_status"] = audit_status

    origins = _split_refs(existing.get("origin_refs", ""))
    origins.add(origin_ref)
    existing["origin_refs"] = _join_refs(origins)

    source_refs = _split_refs(existing.get("source_refs", ""))
    if source_ref:
        source_refs.add(source_ref)
    source_refs.update(_split_refs(entry.get("source_refs", "")))
    existing["source_refs"] = _join_refs(source_refs)

    signal_refs = _split_refs(existing.get("signal_refs", ""))
    if signal_ref:
        signal_refs.add(signal_ref)
    signal_refs.update(_split_refs(entry.get("signal_refs", "")))
    existing["signal_refs"] = _join_refs(signal_refs)

    if not existing.get("audit_status"):
        existing["audit_status"] = "valid"


def load_appendix(path: Path, *, include_current: bool = False) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not path.exists():
        LOG.warning("appendix CSV not found: %s", path)
        return rows
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            if not include_current and r.get("scope") == "current_registry":
                continue
            ds_id = r.get("dataset_id", "").strip()
            if not ds_id:
                continue
            audit = r.get("audit_status", "")
            if audit in ("dead_404", "forbidden_403"):
                continue
            r["origin_refs"] = _join_refs(_split_refs(r.get("origin_refs", "")) | {"appendix"})
            rows.append(r)
    return rows


def load_audit_json(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        LOG.warning("audit JSON not found: %s", path)
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    entries = raw.get("valid_datasets_flat", [])
    if not isinstance(entries, list):
        raise ValueError(f"audit JSON valid_datasets_flat is not a list: {path}")

    rows: list[dict[str, str]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        dataset_id = str(entry.get("id", "")).strip().lower()
        if not dataset_id:
            continue
        rows.append(
            {
                "dataset_id": dataset_id,
                "name": str(entry.get("name", "")).strip(),
                "sector_or_category": str(entry.get("sector", "")).strip(),
                "scope": "colombia_open_data_audit",
                "recommendation": "candidate",
                "relevance": "unknown",
                "audit_status": "valid",
                "origin_refs": "audit_json",
                "url": str(entry.get("url", "")).strip()
                or f"https://{DEFAULT_DOMAIN}/d/{dataset_id}",
            }
        )
    return rows


def load_source_registry(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        LOG.warning("source registry CSV not found: %s", path)
        return []

    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            source_id = (r.get("source_id") or "").strip()
            candidate_ids = _extract_socrata_ids(
                r.get("primary_url"),
                r.get("last_seen_url"),
                r.get("notes"),
            )
            for dataset_id in candidate_ids:
                rows.append(
                    {
                        "dataset_id": dataset_id,
                        "name": (r.get("name") or "").strip(),
                        "sector_or_category": (r.get("category") or "").strip(),
                        "scope": "source_registry",
                        "recommendation": "keep",
                        "relevance": (r.get("signal_promotion_state") or "").strip() or "registry",
                        "audit_status": "valid",
                        "source_refs": source_id,
                        "origin_refs": "source_registry",
                        "url": f"https://{DEFAULT_DOMAIN}/d/{dataset_id}",
                    }
                )
    return rows


def load_signal_source_ids(path: Path) -> set[str]:
    if not path.exists():
        LOG.warning("signal dependency file not found: %s", path)
        return set()
    text = path.read_text(encoding="utf-8")
    source_ids: set[str] = set()
    for match in re.finditer(r"\b(?:sources|required|optional):\s*\[([^\]]*)\]", text):
        for value in match.group(1).split(","):
            source_id = value.strip().strip("'\"")
            if source_id:
                source_ids.add(source_id)
    return source_ids


def load_pipeline_env_sources(pipelines_dir: Path) -> list[dict[str, str]]:
    """Read env-backed Socrata dataset IDs from pipeline class declarations.

    Some backlog lake-only pipelines intentionally keep the Socrata ID in an
    environment variable. This keeps those datasets in the qualification gate
    when the variable is present without hardcoding secrets or IDs in tests.
    """
    import ast

    if not pipelines_dir.exists():
        LOG.warning("pipeline directory not found: %s", pipelines_dir)
        return []

    rows: list[dict[str, str]] = []
    for path in sorted(pipelines_dir.glob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            LOG.warning("failed to parse pipeline file %s: %s", path, exc)
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            attrs: dict[str, str] = {}
            for stmt in node.body:
                if not isinstance(stmt, ast.Assign):
                    continue
                for target in stmt.targets:
                    if not isinstance(target, ast.Name):
                        continue
                    if target.id not in {"name", "source_id", "socrata_dataset_id_env"}:
                        continue
                    if isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str):
                        attrs[target.id] = stmt.value.value
            env_name = attrs.get("socrata_dataset_id_env", "")
            dataset_id = os.environ.get(env_name, "").strip().lower() if env_name else ""
            if not dataset_id or not SOCRATA_ID_RE.fullmatch(dataset_id):
                continue
            source_id = attrs.get("source_id") or attrs.get("name") or node.name
            rows.append(
                {
                    "dataset_id": dataset_id,
                    "name": source_id,
                    "sector_or_category": "pipeline_env",
                    "scope": "pipeline_env",
                    "recommendation": "candidate",
                    "relevance": "env_configured",
                    "audit_status": "valid",
                    "source_refs": source_id,
                    "origin_refs": f"pipeline_env:{path.stem}",
                    "url": f"https://{DEFAULT_DOMAIN}/d/{dataset_id}",
                }
            )
    return rows


def load_known_dataset_entries(
    *,
    appendix_path: Path,
    audit_json_path: Path,
    source_registry_path: Path,
    signal_deps_path: Path,
    pipelines_dir: Path,
    include_current: bool,
    include_appendix: bool,
    include_audit_json: bool,
    include_source_registry: bool,
    include_pipeline_env: bool,
) -> list[dict[str, str]]:
    entries_by_id: dict[str, dict[str, str]] = {}

    source_to_dataset_ids: dict[str, set[str]] = {}

    def add_entries(entries: list[dict[str, str]], fallback_origin: str) -> None:
        for entry in entries:
            source_refs = _split_refs(entry.get("source_refs", ""))
            origin_ref = entry.get("origin_refs", "") or fallback_origin
            _merge_entry(
                entries_by_id,
                entry,
                origin_ref=origin_ref,
                source_ref="",
            )
            dataset_id = entry.get("dataset_id", "").strip().lower()
            for source_id in source_refs:
                source_to_dataset_ids.setdefault(source_id, set()).add(dataset_id)

    if include_appendix:
        add_entries(load_appendix(appendix_path, include_current=include_current), "appendix")
    if include_audit_json:
        add_entries(load_audit_json(audit_json_path), "audit_json")
    if include_source_registry:
        source_registry_entries = load_source_registry(source_registry_path)
        add_entries(source_registry_entries, "source_registry")
    if include_pipeline_env:
        add_entries(load_pipeline_env_sources(pipelines_dir), "pipeline_env")

    signal_source_ids = load_signal_source_ids(signal_deps_path)
    for source_id in signal_source_ids:
        for dataset_id in source_to_dataset_ids.get(source_id, set()):
            entry = entries_by_id.get(dataset_id)
            if not entry:
                continue
            _merge_entry(
                entries_by_id,
                {"dataset_id": dataset_id},
                origin_ref="signal_deps",
                signal_ref=source_id,
            )

    return sorted(entries_by_id.values(), key=lambda row: row["dataset_id"])


def read_catalog(path: Path) -> list[TriageCatalogRow]:
    if not path.exists():
        LOG.warning("catalog CSV not found for --llm-only: %s", path)
        return []
    rows: list[TriageCatalogRow] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            probe_notes = [
                part.strip()
                for part in (r.get("probe_notes") or "").split(";")
                if part.strip()
            ]
            row_kwargs = {
                field_name: r.get(field_name, "")
                for field_name in CATALOG_FIELDS
                if field_name != "probe_notes"
            }
            for int_field in (
                "rows",
                "last_update_days",
                "n_columns",
                "n_meaningful_columns",
                "join_keys_found",
            ):
                try:
                    row_kwargs[int_field] = int(row_kwargs.get(int_field) or -1)
                except (TypeError, ValueError):
                    row_kwargs[int_field] = -1
            rows.append(TriageCatalogRow(**row_kwargs, probe_notes=probe_notes))
    return rows


def write_catalog(rows: list[TriageCatalogRow], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CATALOG_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_csv_row())


def write_proven(rows: list[TriageCatalogRow], out_path: Path) -> None:
    proven = [r for r in rows if r.join_keys_found > 0]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "dataset_id", "name", "sector", "recommendation", "relevance",
        "rows", "join_key_classes", "join_key_columns",
        "sample_join_density", "ingest_class", "source_refs", "origin_refs",
        "signal_refs", "url",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in proven:
            d = row.as_csv_row()
            d["ingest_class"] = classify_dataset(row)
            writer.writerow({f: d.get(f, "") for f in fields})


def write_report(rows: list[TriageCatalogRow], report_path: Path) -> None:
    classified: dict[str, list[TriageCatalogRow]] = {}
    for row in rows:
        cls = classify_dataset(row)
        classified.setdefault(cls, []).append(row)

    class_order = [
        "ingest_priority",
        "ingest",
        "ingest_if_useful",
        "context_enrichment",
        "context_if_useful",
        "schema_core_join",
        "schema_context_join",
        "weak_join",
        "large_no_join",
        "no_join",
        "unknown",
        "unreachable",
    ]

    lines: list[str] = [
        "# Dataset join-key triage report",
        "",
        f"Generated: {datetime.now(tz=UTC).isoformat(timespec='seconds')}",
        "Input: configured dataset source inventories",
        f"Total probed: {len(rows)}",
        f"Datasets with proven join keys: {sum(1 for r in rows if r.join_keys_found > 0)}",
        "",
        "## Ingest class counts",
        "",
    ]
    for cls in class_order:
        lines.append(f"- {cls}: {len(classified.get(cls, []))}")
    lines.append("")

    class_descriptions = {
        "ingest_priority": "2+ core join keys (NIT/contract/process/entity) + >=1K rows; highest priority for pipeline ingestion",
        "ingest": "1+ core join key + >=5K rows; strong candidate for pipeline ingestion",
        "ingest_if_useful": "1+ core join key but <5K rows; ingest only if fills a signal dependency gap",
        "context_enrichment": "BPIN/divipola join key + >=1K rows; useful as enrichment/context layer",
        "context_if_useful": "BPIN/divipola join key but <1K rows; ingest only if context fills a gap",
        "schema_core_join": "Metadata-only pass found a core join key; needs row count/freshness before promotion",
        "schema_context_join": "Metadata-only pass found only BPIN/divipola context keys; needs explicit signal use",
        "weak_join": "Join key found but no core keys and no strong context keys; low priority",
        "large_no_join": ">10K rows and readable schema but no recognized join key; needs manual column review",
        "no_join": "No recognized join key found; likely not useful for entity-linked pipeline",
        "unknown": "Metadata fetch failed; cannot classify",
        "unreachable": "Dataset is 404/403 and cannot be probed",
    }

    for cls in class_order:
        group = classified.get(cls, [])
        if not group:
            continue
        lines.append(f"## {cls}")
        lines.append("")
        lines.append(f"{class_descriptions.get(cls, '')}")
        lines.append("")
        if cls in (
            "ingest_priority",
            "ingest",
            "ingest_if_useful",
            "context_enrichment",
            "schema_core_join",
            "schema_context_join",
        ):
            lines.append("| dataset_id | rows | join_keys | join_columns | density_sample | name |")
            lines.append("|---|---:|---|---|---|---|")
            for r in sorted(group, key=lambda x: -max(x.rows, 0)):
                lines.append(
                    f"| `{r.dataset_id}` | {r.rows:,} | {r.join_key_classes} | "
                    f"{r.join_key_columns} | {r.sample_join_density or '—'} | {r.name} |"
                )
        elif cls in ("weak_join", "large_no_join", "no_join"):
            lines.append("| dataset_id | rows | cols | join_keys | name |")
            lines.append("|---|---:|---:|---|---|")
            for r in sorted(group, key=lambda x: -max(x.rows, 0)):
                lines.append(
                    f"| `{r.dataset_id}` | {r.rows:,} | {r.n_meaningful_columns} | "
                    f"{r.join_key_classes or '—'} | {r.name} |"
                )
        lines.append("")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--appendix",
        default=str(REPO_ROOT / "docs/datasets/dataset_relevance_appendix.csv"),
        help="Path to the relevance appendix CSV",
    )
    parser.add_argument(
        "--audit-json",
        default=str(REPO_ROOT / "docs/datasets/colombia_open_data_audit.json"),
        help="Path to the Colombia open-data audit JSON",
    )
    parser.add_argument(
        "--source-registry",
        default=str(REPO_ROOT / "docs/source_registry_co_v1.csv"),
        help="Path to the current source registry CSV",
    )
    parser.add_argument(
        "--signal-deps",
        default=str(REPO_ROOT / "config/signal_source_deps.yml"),
        help="Path to signal dependency registry",
    )
    parser.add_argument(
        "--pipelines-dir",
        default=str(REPO_ROOT / "etl/src/coacc_etl/pipelines"),
        help="Path to ETL pipeline modules for env-backed Socrata IDs",
    )
    parser.add_argument(
        "--catalog-out",
        default=str(REPO_ROOT / "docs/datasets/source_qualification_catalog.csv"),
        help="Output: full catalog CSV with all probed metadata",
    )
    parser.add_argument(
        "--proven-out",
        default=str(REPO_ROOT / "docs/datasets/source_qualification_proven.csv"),
        help="Output: only datasets with proven join keys",
    )
    parser.add_argument(
        "--report-out",
        default=str(REPO_ROOT / "docs/datasets/source_qualification_report.md"),
        help="Output: human-readable markdown report",
    )
    parser.add_argument(
        "--domain",
        default=DEFAULT_DOMAIN,
        help="Socrata domain (default www.datos.gov.co)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Only probe first N candidates")
    parser.add_argument("--sleep", type=float, default=0.25, help="Seconds between requests")
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout seconds")
    parser.add_argument("--count-timeout", type=float, default=20.0, help="Timeout for count(*) queries")
    parser.add_argument("--skip-count", action="store_true", help="Skip count(*) queries entirely")
    parser.add_argument("--probe-sample", type=int, default=5, help="Rows to sample for join density")
    parser.add_argument("--include-current", action="store_true", help="Also probe current_registry sources")
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Only use Socrata metadata/schema; skip count(*) and row sampling",
    )
    parser.add_argument(
        "--all-known",
        action="store_true",
        help="Use appendix + audit JSON + source registry + env-backed pipeline IDs",
    )
    parser.add_argument(
        "--no-appendix",
        action="store_true",
        help="With --all-known, skip dataset_relevance_appendix.csv",
    )
    parser.add_argument(
        "--no-audit-json",
        action="store_true",
        help="With --all-known, skip colombia_open_data_audit.json",
    )
    parser.add_argument(
        "--no-source-registry",
        action="store_true",
        help="With --all-known, skip docs/source_registry_co_v1.csv",
    )
    parser.add_argument(
        "--no-pipeline-env",
        action="store_true",
        help="With --all-known, skip env-backed pipeline dataset IDs",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--llm-review", action="store_true",
        help="Run LLM-powered review on datasets with ≤1 join key found",
    )
    parser.add_argument(
        "--llm-provider", default="gemini",
        help="LLM provider: gemini, anthropic, or openai (default: gemini)",
    )
    parser.add_argument(
        "--llm-model", default="",
        help="LLM model name (default depends on provider)",
    )
    parser.add_argument(
        "--llm-api-key", default="",
        help="LLM API key (or set provider env var such as GEMINI_API_KEY)",
    )
    parser.add_argument(
        "--llm-cache",
        default=str(REPO_ROOT / "docs/datasets/source_qualification_llm_cache.json"),
        help="Path to LLM review cache file",
    )
    parser.add_argument(
        "--llm-required",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail when --llm-review is requested but no LLM results are produced",
    )
    parser.add_argument(
        "--llm-sleep", type=float, default=0.5,
        help="Seconds between LLM API calls (default: 0.5)",
    )
    parser.add_argument(
        "--llm-min-confidence",
        type=float,
        default=0.75,
        help="Minimum confidence required to accept an LLM join-key finding",
    )
    parser.add_argument(
        "--llm-only", action="store_true",
        help="Only run LLM review (skip Socrata probing, use existing catalog)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    env_path = REPO_ROOT / ".env"
    _load_env_file(env_path, override=True)

    appendix_path = Path(args.appendix)
    audit_json_path = Path(args.audit_json)
    source_registry_path = Path(args.source_registry)
    signal_deps_path = Path(args.signal_deps)
    pipelines_dir = Path(args.pipelines_dir)

    if args.llm_only:
        entries = []
    elif args.all_known:
        entries = load_known_dataset_entries(
            appendix_path=appendix_path,
            audit_json_path=audit_json_path,
            source_registry_path=source_registry_path,
            signal_deps_path=signal_deps_path,
            pipelines_dir=pipelines_dir,
            include_current=args.include_current,
            include_appendix=not args.no_appendix,
            include_audit_json=not args.no_audit_json,
            include_source_registry=not args.no_source_registry,
            include_pipeline_env=not args.no_pipeline_env,
        )
    else:
        if not appendix_path.is_file():
            LOG.error("appendix CSV not found: %s", appendix_path)
            return 2
        entries = load_appendix(appendix_path, include_current=args.include_current)
    if args.limit and not args.llm_only:
        entries = entries[: args.limit]

    if args.metadata_only:
        args.skip_count = True
        args.probe_sample = 0

    if args.llm_only:
        LOG.info("loading existing catalog for LLM-only review: %s", args.catalog_out)
    else:
        LOG.info(
            "probing %d candidate datasets from %s",
            len(entries),
            "all known sources" if args.all_known else str(appendix_path),
        )

    rows: list[TriageCatalogRow] = []
    catalog_path = Path(args.catalog_out)
    proven_path = Path(args.proven_out)
    report_path = Path(args.report_out)

    def _flush() -> None:
        write_catalog(rows, catalog_path)
        write_proven(rows, proven_path)
        write_report(rows, report_path)

    if args.llm_only:
        rows = read_catalog(catalog_path)
    else:
        with _build_client(args.timeout) as meta_client:
            count_client = None
            if not args.skip_count and args.count_timeout > args.timeout:
                count_client = _build_client(args.count_timeout)

            try:
                for i, entry in enumerate(entries, start=1):
                    try:
                        row = probe_dataset(
                            meta_client,
                            entry,
                            domain=args.domain,
                            probe_sample=args.probe_sample,
                            skip_count=args.skip_count,
                            count_client=count_client,
                        )
                    except Exception as exc:
                        row = TriageCatalogRow(
                            dataset_id=entry.get("dataset_id", ""),
                            name=entry.get("name", ""),
                            sector=entry.get("sector_or_category", ""),
                            scope=entry.get("scope", ""),
                            recommendation=entry.get("recommendation", ""),
                            relevance=entry.get("relevance", ""),
                            audit_status=entry.get("audit_status", ""),
                            source_refs=entry.get("source_refs", ""),
                            origin_refs=entry.get("origin_refs", ""),
                            signal_refs=entry.get("signal_refs", ""),
                            url=entry.get("url", ""),
                            probe_notes=[f"probe failed: {exc}"],
                        )
                    rows.append(row)
                    LOG.info(
                        "[%d/%d] %s rows=%s join_keys=%s classes=%s cols=%s/%s",
                        i,
                        len(entries),
                        row.dataset_id,
                        row.rows,
                        row.join_keys_found,
                        row.join_key_classes,
                        row.n_meaningful_columns,
                        row.n_columns,
                    )
                    if i % 50 == 0:
                        _flush()
                        LOG.info("flushed partial output at %d/%d", i, len(entries))
                    if args.sleep > 0:
                        time.sleep(args.sleep)
            finally:
                if count_client is not None:
                    count_client.close()

        _flush()

    if args.llm_review or args.llm_only:
        review_targets = [r for r in rows if r.join_keys_found <= 1 and r.audit_status not in ("dead_404", "forbidden_403")]
        LOG.info(
            "running LLM review on %d datasets (with ≤1 join key)",
            len(review_targets),
        )
        if not review_targets:
            LOG.warning("no datasets to review with LLM")
        else:
            with _build_client(args.timeout) as review_client:
                llm_results = _try_llm_review(
                    review_targets,
                    provider=args.llm_provider,
                    model=args.llm_model,
                    api_key=args.llm_api_key or None,
                    sleep_between=args.llm_sleep,
                    cache_path=Path(args.llm_cache),
                )
                if args.llm_required and not llm_results:
                    LOG.error(
                        "LLM review is required but produced no results; "
                        "check provider, API key, model, network, and quota"
                    )
                    return 3
                rows = _apply_llm_findings(
                    rows, llm_results,
                    client=review_client,
                    domain=args.domain,
                    probe_sample=args.probe_sample,
                    min_confidence=args.llm_min_confidence,
                )
            _flush()
            llm_key_count = sum(1 for r in rows if "llm_added" in " ".join(r.probe_notes))
            llm_tier_count = sum(1 for r in rows if any("llm_tier" in n for n in r.probe_notes))
            LOG.info("LLM review: added keys to %d datasets, classified %d", llm_key_count, llm_tier_count)

    ingest_classes = {"ingest_priority", "ingest", "ingest_if_useful"}
    schema_classes = {"schema_core_join", "schema_context_join"}
    proven_count = sum(1 for r in rows if r.join_keys_found > 0)
    ingest_count = sum(1 for r in rows if classify_dataset(r) in ingest_classes)
    schema_count = sum(1 for r in rows if classify_dataset(r) in schema_classes)
    LOG.info("wrote %s, %s, %s", args.catalog_out, args.proven_out, args.report_out)
    LOG.info("datasets with join keys: %d / %d", proven_count, len(rows))
    LOG.info("datasets recommended for ingestion: %d", ingest_count)
    LOG.info("datasets schema-qualified pending row/freshness checks: %d", schema_count)

    return 0


if __name__ == "__main__":
    sys.exit(main())
