"""Runtime authority over the signed dataset catalog.

The catalog package reads ``etl/datasets/<id>.yml`` contracts that were
bootstrapped from ``docs/datasets/catalog.signed.csv`` + ``catalog.proven.csv``.
"""

from coacc_etl.catalog.loader import clear_cache, datasets_dir, load_catalog
from coacc_etl.catalog.models import Adapter, DatasetSpec, JoinKeyClass, Tier

__all__ = [
    "Adapter",
    "DatasetSpec",
    "JoinKeyClass",
    "Tier",
    "clear_cache",
    "datasets_dir",
    "load_catalog",
]
