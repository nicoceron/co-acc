"""LLM-powered review pass for datasets the deterministic pass missed.

Used as a second-review gate when ``--llm-review`` is requested. Supports
Anthropic, OpenAI, and Gemini (the default; cheapest-per-token of the
three for this workload).

A row only earns a new join key if (a) the LLM proposes it with confidence
≥ ``min_confidence``, AND (b) the column actually exists in the row's
``columns_all``, AND (c) :func:`promotion._is_valid_llm_join_key` accepts
the (column, class) pair. The LLM cannot route around the deterministic
guardrail.
"""
# ruff: noqa: E501
from __future__ import annotations

import json
import logging
import os
import time
from typing import TYPE_CHECKING, Any

import httpx

from coacc_etl.qualification.promotion import (
    JOIN_KEY_CLASSES,
    TriageCatalogRow,
    _is_valid_llm_join_key,
    _normalize_col,
    classify_dataset,
)
from coacc_etl.qualification.socrata_probe import _probe_join_density

if TYPE_CHECKING:
    from pathlib import Path

LOG = logging.getLogger("source_qualification")

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
