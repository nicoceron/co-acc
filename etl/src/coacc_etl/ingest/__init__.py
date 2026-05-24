"""Generic YAML-driven ingest for signed-catalog datasets.

``ingest.socrata`` is the only supported ingest path for ``tier: core`` datasets.
Bespoke non-Socrata adapters live under ``ingest.custom`` and are backlog.
"""
from coacc_etl.ingest.coverage import CoverageFailure, assert_coverage
from coacc_etl.ingest.socrata import IngestError, IngestResult, SocrataClient, ingest

__all__ = [
    "CoverageFailure",
    "IngestError",
    "IngestResult",
    "SocrataClient",
    "assert_coverage",
    "ingest",
]
