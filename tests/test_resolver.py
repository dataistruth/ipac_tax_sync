"""Tests for ipac_delta_sync config/common model."""

from util.bundle_config import (
    resolve_dest_schema_suffix,
    resolve_num_of_tables_in_pipeline,
    resolve_uc_catalog,
    uc_catalog_var_ref,
)
from util.config_loader import get_client, load_client_overrides, load_common_tables, load_client_registry
from util.pipeline_generator import (
    chunk_tables,
    generate_client_pipelines_yaml,
    generate_lakeflow_pipeline_yaml,
)
from util.resolver import resolve_effective_tables
from util.schema_generator import generate_schema_resource_yaml
from util.sql_generator import generate_enable_ct_sql

UC_REF = uc_catalog_var_ref()


def _active_clients():
    registry = load_client_registry()
    return [c for c in registry.clients if c.is_active]


def test_active_client_count():
    assert len(_active_clients()) == 2


def test_client_names_present():
    names = {c.client_nm for c in _active_clients()}
    assert "iPC_2025_Dev7_15350" in names
    assert "iPC_2025_Dev7_15447" in names


def test_common_tables_load_and_recon_type_exists():
    catalog = load_common_tables()
    assert len(catalog.tables) > 150
    first = catalog.tables[0]
    assert first.recon_type in (1, 2, 3)
    assert first.scd_type in (1, 2)


def test_resolve_effective_tables_for_first_client():
    catalog = load_common_tables()
    client = _active_clients()[0]
    overrides = load_client_overrides(client.client_nm)
    tables = resolve_effective_tables(client, catalog, overrides)

    assert len(tables) == len([t for t in catalog.tables if t.is_active])
    assert all(t.recon_type in (1, 2, 3) for t in tables)
    assert all(t.src_schema == "dbo" for t in tables)


def test_dest_schema_suffix_default_empty():
    assert resolve_dest_schema_suffix() == "poc_1"


def test_generate_yaml_uses_client_name_as_schema_when_suffix_empty():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15350")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    yaml_text = generate_lakeflow_pipeline_yaml(
        client,
        tables,
        uc_catalog_ref=UC_REF,
        resolved_uc_catalog=resolve_uc_catalog(),
        num_of_tables_in_pipeline=resolve_num_of_tables_in_pipeline(),
        dest_schema_suffix="",
    )

    assert f"schema: {client.client_nm}" in yaml_text
    assert f"schema_name: {client.client_nm}" in yaml_text
    assert f"destination_schema: '{client.client_nm}'" in yaml_text
    assert "serverless: false" in yaml_text


def test_generate_yaml_uses_suffix_when_provided():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15447")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    yaml_text = generate_lakeflow_pipeline_yaml(
        client,
        tables,
        uc_catalog_ref=UC_REF,
        dest_schema_suffix="_raw",
    )

    assert f"schema: {client.client_nm}_raw" in yaml_text
    assert f"schema_name: {client.client_nm}_raw" in yaml_text
    assert f"destination_schema: '{client.client_nm}_raw'" in yaml_text


def test_chunk_tables_respects_batch_size():
    batches = chunk_tables(list(range(203)), 5)
    assert len(batches) == 41
    assert len(batches[0]) == 5
    assert len(batches[-1]) == 3


def test_generate_yaml_splits_into_multiple_pipelines():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15350")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    yaml_text = generate_client_pipelines_yaml(
        client,
        tables,
        uc_catalog_ref=UC_REF,
        num_of_tables_in_pipeline=5,
        dest_schema_suffix="",
    )

    assert "resources:" in yaml_text
    assert "pipelines:" in yaml_text
    assert "p_iPC_2025_Dev7_15350_1:" in yaml_text


def test_enable_ct_sql_includes_grants_when_grantee_set():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15350")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    sql_text = generate_enable_ct_sql(client, tables[:1], ct_grantee="AppUser_Test")

    assert "GRANT VIEW DATABASE STATE TO [AppUser_Test];" in sql_text
    assert "GRANT SELECT, VIEW CHANGE TRACKING ON [dbo].[" in sql_text


def test_schema_resource_yaml_format():
    text = generate_schema_resource_yaml(
        schema_name="iPC_2025_Dev7_15350_raw",
        uc_catalog_ref="${var.uc_catalog}",
        comment="schema test",
    )
    assert "resources:" in text
    assert "schemas:" in text
    assert "name: iPC_2025_Dev7_15350_raw" in text
    assert "catalog_name: ${var.uc_catalog}" in text
