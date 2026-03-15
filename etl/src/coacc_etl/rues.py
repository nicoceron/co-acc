from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

import httpx
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

RUES_ELASTIC_BASE_URL = "https://elasticprd.rues.org.co"
RUES_SHARED_SECRET = "ac1244b5-8bee-47b2-a4a5-924a748d907f"


def _evp_bytes_to_key(
    password: bytes,
    salt: bytes,
    key_len: int,
    iv_len: int,
) -> tuple[bytes, bytes]:
    derived = b""
    block = b""
    while len(derived) < key_len + iv_len:
        block = hashlib.md5(block + password + salt).digest()
        derived += block
    return derived[:key_len], derived[key_len : key_len + iv_len]


def encrypt_rues_payload(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode(
        "utf-8"
    )
    pad_size = 16 - (len(serialized) % 16)
    padded = serialized + bytes([pad_size]) * pad_size
    salt = get_random_bytes(8)
    key, iv = _evp_bytes_to_key(RUES_SHARED_SECRET.encode("utf-8"), salt, 32, 16)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    ciphertext = cipher.encrypt(padded)
    return base64.b64encode(b"Salted__" + salt + ciphertext).decode("ascii")


class RuesElasticClient:
    """Small client for public RUES elastic endpoints exposed by the web frontend."""

    def __init__(
        self,
        *,
        base_url: str = RUES_ELASTIC_BASE_URL,
        timeout: float = 30.0,
    ) -> None:
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "coacc-etl/0.1",
            },
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> RuesElasticClient:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _post_json(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self._client.post(path, json=payload or {})
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            return data
        raise ValueError(f"Unexpected RUES response payload at {path}: {type(data)!r}")

    def _post_encrypted(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post_json(path, {"dataBody": encrypt_rues_payload(payload)})

    def list_chamber_catalog(self) -> dict[str, Any]:
        return self._post_json("/api/ListarComboCamaras")

    def list_chamber_details(self) -> dict[str, Any]:
        return self._post_json("/api/ListaCamara")

    def lookup_business_group(self, nit: str) -> dict[str, Any]:
        return self._post_encrypted("/api/ConsultarGrupoEmpresariales", {"NIT": nit})
