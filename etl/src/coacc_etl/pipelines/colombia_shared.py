from __future__ import annotations

import ast
import hashlib
import re
import unicodedata
from typing import Any

import pandas as pd

from coacc_etl.transforms import normalize_name, parse_date, strip_document

_EMPTY_MARKERS = {
    "",
    "N/A",
    "NA",
    "NAN",
    "NO APLICA",
    "NO APLICA/NO PERTENECE",
    "NO DEFINIDO",
    "NO DEFINIDO/NO PERTENECE",
    "NO DEFINIDO/NO PERTENECE ",
    "NO PROVISTO",
    "NO APLICA ",
    "NO DEFINIDO ",
    "NO DEFINIDO.",
    "NO DEFINIDO/NO APLICA",
    "NO DEFINIDO/NO APLICA ",
    "NO DEFINIDO/NO APLICA.",
    "NO DEFINIDO/NO PERTENECE.",
    "NO DEFINIDO/NO PERTENECE/NO APLICA",
    "NO DISPONIBLE",
    "NO REGISTRA",
    "NO TIENE",
    "NONE",
    "NULL",
    "NULL ",
    "SIN INFORMACION",
    "SIN INFORMACIÓN",
    "UNSPECIFIED",
}

_TRUE_MARKERS = {"SI", "SÍ", "TRUE", "YES", "Y", "1"}
_FALSE_MARKERS = {"NO", "FALSE", "N", "0"}


def clean_text(raw: object) -> str:
    if raw is None:
        return ""
    if isinstance(raw, dict):
        if "url" in raw:
            return clean_text(raw.get("url"))
        raw = str(raw)
    value = str(raw).replace("\xa0", " ").strip()
    if not value:
        return ""
    normalized = re.sub(r"\s+", " ", value).strip()
    if normalized.upper() in _EMPTY_MARKERS:
        return ""
    return normalized


def clean_name(raw: object) -> str:
    value = clean_text(raw)
    return normalize_name(value) if value else ""


def extract_url(raw: object) -> str:
    if isinstance(raw, dict):
        return clean_text(raw.get("url"))

    value = clean_text(raw)
    if not value or not value.startswith("{"):
        return value

    try:
        parsed = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return value

    if isinstance(parsed, dict):
        return clean_text(parsed.get("url"))
    return value


def parse_amount(raw: object) -> float | None:
    value = clean_text(raw)
    if not value:
        return None

    cleaned = re.sub(r"[^0-9,.\-]", "", value)
    if not cleaned or cleaned in {"-", ".", ","}:
        return None

    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        parts = cleaned.split(",")
        cleaned = ".".join(parts) if len(parts) == 2 and len(parts[-1]) <= 2 else "".join(parts)
    elif "." in cleaned:
        parts = cleaned.split(".")
        if len(parts) > 2 or (len(parts) == 2 and len(parts[-1]) == 3):
            cleaned = "".join(parts)

    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_integer(raw: object) -> int | None:
    digits = strip_document(clean_text(raw))
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def parse_flag(raw: object) -> bool | None:
    value = clean_text(raw).upper()
    if not value:
        return None
    if value in _TRUE_MARKERS:
        return True
    if value in _FALSE_MARKERS:
        return False
    return None


def build_person_name(*parts: object) -> str:
    values = [clean_text(part) for part in parts if clean_text(part)]
    if not values:
        return ""
    return normalize_name(" ".join(values))


def stable_id(prefix: str, *parts: object) -> str:
    payload = "|".join(clean_text(part) for part in parts if clean_text(part))
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def make_public_office_id(
    document_id: str,
    institution_code: object,
    institution_name: object,
    role_name: object,
    dependency_name: object,
) -> str:
    return stable_id(
        "office",
        document_id,
        institution_code,
        institution_name,
        role_name,
        dependency_name,
    )


def parse_iso_date(raw: object) -> str | None:
    value = clean_text(raw)
    if not value:
        return None
    return parse_date(value)


def normalize_column_name(raw: object) -> str:
    value = unicodedata.normalize("NFKD", str(raw or ""))
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized_columns = [normalize_column_name(column) for column in df.columns]
    if list(df.columns) == normalized_columns:
        return df

    normalized = df.copy()
    normalized.columns = normalized_columns
    return normalized


def read_csv_normalized(path: str, **kwargs: Any) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, **kwargs)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected DataFrame, got {type(df)}")
    return normalize_dataframe_columns(df)


def read_csv_normalized_with_fallback(
    path: str,
    *,
    encodings: tuple[str, ...] = ("utf-8-sig", "utf-8", "cp1252", "latin-1"),
    **kwargs: Any,
) -> pd.DataFrame:
    last_error: UnicodeDecodeError | None = None
    for encoding in encodings:
        try:
            df = pd.read_csv(path, encoding=encoding, **kwargs)
            if not isinstance(df, pd.DataFrame):
                continue
            return normalize_dataframe_columns(df)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    try:
        df = pd.read_csv(path, **kwargs)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected DataFrame, got {type(df)}")
    return normalize_dataframe_columns(df)
