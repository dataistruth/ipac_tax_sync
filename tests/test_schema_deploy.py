"""Tests for bundle schema deploy selector collection."""

from pathlib import Path

from util.schema_deploy import collect_schema_deploy_selectors, schema_resource_keys_from_yaml


def test_schema_resource_keys_from_yaml():
    path = Path("resources/schemas/ipac_metadata_schema.yml")
    keys = schema_resource_keys_from_yaml(path)
    assert keys == ["schema_ipac_metadata"]


def test_collect_schema_deploy_selectors_includes_metadata_and_client():
    selectors = collect_schema_deploy_selectors(
        Path("generated/schema"),
        Path("resources/schemas"),
    )
    assert "schemas.schema_ipac_metadata" in selectors
    assert any(s.startswith("schemas.schema_ipc_2025_dev7_") for s in selectors)
