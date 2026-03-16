from __future__ import annotations

from pathlib import Path

import pytest

from coacc_etl.pipelines.secop_integrado import SecopIntegratedPipeline


@pytest.mark.integration
def test_secop_integrated_pipeline_load(neo4j_driver, tmp_path):
    # Setup paths
    fixtures_dir = Path(__file__).parent.parent / "fixtures"
    data_dir = fixtures_dir

    # Initialize pipeline
    pipeline = SecopIntegratedPipeline(neo4j_driver, data_dir=data_dir)

    # Run ETL cycle
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    # Verify results in Neo4j
    with neo4j_driver.session() as session:
        # Check for Companies
        result = session.run("MATCH (c:Company) RETURN count(c) AS count")
        company_count = result.single()["count"]
        assert company_count >= 2 # CONSORCIO ANDINO and SALUD ABIERTA

        # Check for specific company by NIT
        result = session.run("MATCH (c:Company {document_id: '901234567'}) RETURN c.name AS name")
        record = result.single()
        assert record["name"] == "CONSORCIO ANDINO S.A.S."

        # Check for Contracts and Relationships
        result = session.run(
            "MATCH (buyer)-[r:CONTRATOU]->(supplier:Company) RETURN count(r) AS count"
        )
        rel_count = result.single()["count"]
        assert rel_count >= 2

        # Verify contract properties on the edge
        result = session.run("""
            MATCH (b)-[r:CONTRATOU]->(s:Company {document_id: '901234567'})
            RETURN r.total_value AS value, r.city AS city
        """)
        record = result.single()
        assert record["value"] == 1500000000.0
        assert record["city"] == "Medellin"
