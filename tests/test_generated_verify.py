"""Tests for stale generated artifact detection."""

from pathlib import Path

from util.generated_verify import find_embedded_schemas_in_bundle, find_stale_generated_suffix_markers


def test_find_stale_suffix_marker(tmp_path, monkeypatch):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "client_pipeline.yml").write_text("schema_ipc_2025_dev7_15347poc_1\n", encoding="utf-8")
    schema_dir = tmp_path / "schema"
    schema_dir.mkdir()

    monkeypatch.setattr("util.generated_verify.generated_bundle_dir", lambda: bundle)
    monkeypatch.setattr("util.generated_verify.generated_config_schema_dir", lambda: schema_dir)

    hits = find_stale_generated_suffix_markers("_poc1")
    assert len(hits) == 1
    assert "poc_1" in hits[0]


def test_find_embedded_schemas(tmp_path, monkeypatch):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "x_pipeline.yml").write_text(
        "resources:\n  schemas:\n    foo:\n  pipelines:\n    p:\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("util.generated_verify.generated_bundle_dir", lambda: bundle)
    monkeypatch.setattr(
        "util.generated_verify.generated_config_schema_dir",
        lambda: tmp_path / "missing",
    )

    hits = find_embedded_schemas_in_bundle()
    assert len(hits) == 1
