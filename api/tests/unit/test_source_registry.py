from pathlib import Path

from coacc.services import source_registry


def test_load_source_registry_reads_signed_catalog_and_custom_yaml(
    monkeypatch,
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "docs" / "datasets" / "catalog.signed.csv"
    catalog_path.parent.mkdir(parents=True)
    catalog_path.write_text(
        "\n".join([
            "dataset_id,name,sector,recommendation,relevance,audit_status,last_update,update_freq,source_refs,url,probe_notes",
            "abcd-1234,Demo Dataset,contracts,keep,already_used,valid,2026-01-01,daily,source_a|source_b,https://www.datos.gov.co/d/abcd-1234,",
            "efgh-5678,Duplicate Source,contracts,keep,promoted,valid,2026-01-01,monthly,source_a,https://www.datos.gov.co/d/efgh-5678,",
            "wxyz-9999,Ignored Dataset,context,drop,low,valid,2026-01-02,monthly,,https://www.datos.gov.co/d/wxyz-9999,",
        ]),
        encoding="utf-8",
    )
    datasets_dir = tmp_path / "etl" / "datasets"
    datasets_dir.mkdir(parents=True)
    (datasets_dir / "abcd-1234.yml").write_text(
        """
id: abcd-1234
name: Demo Dataset
sector: contracts
tier: core
join_keys: {}
watermark_column: updated_at
partition_column: updated_at
columns_map: {contract_id: contract_id}
freq: daily
url: https://www.datos.gov.co/d/abcd-1234
""".strip(),
        encoding="utf-8",
    )
    (datasets_dir / "custom_feed.yml").write_text(
        """
id: custom_feed
name: Custom Feed
sector: sanctions
tier: core
adapter: paco_sanctions
join_keys: {}
full_refresh_only: true
columns_map: {subject_document_id: subject_document_id}
freq: weekly
url: https://example.test/custom
notes: Custom source.
""".strip(),
        encoding="utf-8",
    )
    lake_root = tmp_path / "lake"
    (lake_root / "raw" / "source=abcd-1234").mkdir(parents=True)
    (lake_root / "raw" / "source=abcd-1234" / "part.parquet").write_bytes(b"")
    (lake_root / "raw" / "source=custom_feed").mkdir(parents=True)
    (lake_root / "raw" / "source=custom_feed" / "part.parquet").write_bytes(b"")

    monkeypatch.setenv("COACC_SOURCE_REGISTRY_PATH", str(catalog_path))
    monkeypatch.setenv("COACC_LAKE_ROOT", str(lake_root))
    monkeypatch.setattr(source_registry, "_DATASET_CONTRACT_DIR", datasets_dir)

    entries = source_registry.load_source_registry()
    by_id = {entry.id: entry for entry in entries}

    assert sorted(by_id) == ["custom_feed", "source_a", "source_b"]
    assert len(entries) == len(by_id)
    assert by_id["source_a"].load_state == "loaded"
    assert by_id["source_a"].pipeline_id == "source_a"
    assert by_id["source_a"].signal_promotion_state == "promoted"
    assert by_id["source_a"].primary_url == "https://www.datos.gov.co/d/abcd-1234"
    assert by_id["custom_feed"].access_mode == "web"
    assert by_id["custom_feed"].public_access_mode == "download"
