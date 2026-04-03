from __future__ import annotations

from typing import Any

from coacc_etl.base import Pipeline
from coacc_etl.pipelines.control_politico_requirements import (
    ControlPoliticoRequirementsPipeline,
)
from coacc_etl.pipelines.control_politico_sessions import ControlPoliticoSessionsPipeline


class ControlPoliticoPipeline(Pipeline):
    name = "control_politico"
    source_id = "control_politico"

    def __init__(
        self,
        driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._pipelines = [
            ControlPoliticoRequirementsPipeline(
                driver=driver,
                data_dir=data_dir,
                limit=limit,
                chunk_size=chunk_size,
                **kwargs,
            ),
            ControlPoliticoSessionsPipeline(
                driver=driver,
                data_dir=data_dir,
                limit=limit,
                chunk_size=chunk_size,
                **kwargs,
            ),
        ]

    def extract(self) -> None:
        self.rows_in = 0
        for pipeline in self._pipelines:
            pipeline.extract()
            self.rows_in += pipeline.rows_in

    def transform(self) -> None:
        for pipeline in self._pipelines:
            pipeline.transform()

    def load(self) -> None:
        self.rows_loaded = 0
        for pipeline in self._pipelines:
            pipeline.load()
            self.rows_loaded += pipeline.rows_loaded
