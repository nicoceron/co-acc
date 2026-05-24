"""YAML-driven ingest for signed-catalog datasets.

Socrata remains the default adapter. Bespoke public-data sources route through
``ingest.custom`` while preserving the same lake result contract.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from coacc_etl.ingest.coverage import CoverageFailure, assert_coverage
from coacc_etl.ingest.socrata import (
    IngestError,
    IngestResult,
    SocrataClient,
)
from coacc_etl.ingest.socrata import (
    ingest as _socrata_ingest,
)

if TYPE_CHECKING:
    from coacc_etl.catalog import DatasetSpec
    from coacc_etl.ingest.socrata import PaginationMode


def ingest(
    spec: DatasetSpec,
    *,
    client: SocrataClient | None = None,
    full_refresh: bool = False,
    page_size: int | None = None,
    max_pages: int | None = None,
    timeout: float | None = None,
    max_timeout: float | None = None,
    pagination: PaginationMode = "offset",
) -> IngestResult:
    """Dispatch one dataset ingest to its declared adapter."""
    if spec.adapter == "socrata":
        return _socrata_ingest(
            spec,
            client=client,
            full_refresh=full_refresh,
            page_size=page_size,
            max_pages=max_pages,
            timeout=timeout,
            max_timeout=max_timeout,
            pagination=pagination,
        )

    if client is not None:
        msg = f"{spec.id}: custom adapters do not accept a SocrataClient"
        raise IngestError(msg)
    if max_pages is not None or max_timeout is not None or pagination != "offset":
        msg = f"{spec.id}: Socrata pagination options do not apply to adapter={spec.adapter!r}"
        raise IngestError(msg)
    if spec.adapter == "paco_sanctions":
        from coacc_etl.ingest.custom.paco_sanctions import ingest as paco_ingest

        return paco_ingest(
            spec,
            full_refresh=full_refresh,
            chunk_size=page_size,
            timeout=timeout,
        )

    msg = f"{spec.id}: unsupported ingest adapter {spec.adapter!r}"
    raise IngestError(msg)


__all__ = [
    "CoverageFailure",
    "IngestError",
    "IngestResult",
    "SocrataClient",
    "assert_coverage",
    "ingest",
]
