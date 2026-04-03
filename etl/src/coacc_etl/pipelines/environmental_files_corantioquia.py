from __future__ import annotations

from coacc_etl.pipelines.environmental_files import EnvironmentalFilesPipeline


class EnvironmentalFilesCorantioquiaPipeline(EnvironmentalFilesPipeline):
    name = "environmental_files_corantioquia"
    source_id = "environmental_files_corantioquia"
