"""Tests for ipac_delta_sync config/common model."""

from util.bundle_config import (
    pipeline_tag_var_ref,
    resolve_dest_schema_suffix,
    resolve_num_of_tables_in_pipeline,
    resolve_uc_catalog,
    uc_catalog_var_ref,
    uc_lkf_staging_schema_var_ref,
)
from util.config_loader import get_client, load_client_overrides, load_common_tables, load_client_registry
from util.pipeline_generator import (
    chunk_tables,
    generate_client_pipelines_yaml,
    generate_lakeflow_pipeline_yaml,
)
from util.pipeline_registry import write_pipeline_name_registry
from util.resolver import resolve_effective_tables
from util.schema_generator import generate_schema_resource_yaml
from util.sql_generator import (
    generate_enable_ct_or_cdc_sql,
    generate_table_pk_ct_status_sql,
    generate_cdc_grants_sql,
    generate_ct_grants_sql,
    write_source_replication_sql,
)

UC_REF = uc_catalog_var_ref()
LKF_SCHEMA_REF = uc_lkf_staging_schema_var_ref()
PIPELINE_TAG_REF = pipeline_tag_var_ref()


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


def test_generate_yaml_uses_per_client_destination_schema_with_suffix():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15350")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    dest_schema = client.raw_schema("poc_1")
    yaml_text = generate_lakeflow_pipeline_yaml(
        client,
        tables,
        uc_catalog_ref=UC_REF,
        resolved_uc_catalog=resolve_uc_catalog(),
        num_of_tables_in_pipeline=resolve_num_of_tables_in_pipeline(),
        dest_schema_suffix="poc_1",
    )

    assert f"schema: {LKF_SCHEMA_REF}" in yaml_text
    assert f"bundle: {PIPELINE_TAG_REF}" in yaml_text
    assert f"destination_schema: '{dest_schema}'" in yaml_text
    assert dest_schema == "iPC_2025_Dev7_15350poc_1"
    assert "data_staging_options:" not in yaml_text
    assert "serverless: false" in yaml_text


def test_generate_yaml_uses_suffix_when_provided():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15447")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    dest_schema = client.raw_schema("_raw")
    yaml_text = generate_lakeflow_pipeline_yaml(
        client,
        tables,
        uc_catalog_ref=UC_REF,
        dest_schema_suffix="_raw",
    )

    assert f"schema: {LKF_SCHEMA_REF}" in yaml_text
    assert f"bundle: {PIPELINE_TAG_REF}" in yaml_text
    assert f"destination_schema: '{dest_schema}'" in yaml_text
    assert "data_staging_options:" not in yaml_text


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


def test_enable_ct_or_cdc_sql_single_block_no_grants():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15350")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    sql_text = generate_enable_ct_or_cdc_sql(client, tables[:1])

    assert "sp_cdc_enable_table" in sql_text
    assert "ENABLE CHANGE_TRACKING" in sql_text
    assert "supports_net_changes = 0" in sql_text
    assert "EXEC(N'ALTER DATABASE" in sql_text
    assert "GRANT " not in sql_text


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


def test_pipeline_registry_written(tmp_path):
    out = write_pipeline_name_registry(tmp_path, ["p_a_2", "p_a_1", "p_a_1"])
    content = (tmp_path / "pipeline_names.json").read_text(encoding="utf-8")
    assert out.endswith("pipeline_names.json")
    assert '"pipelines": [' in content
    assert '"p_a_1"' in content
    assert '"p_a_2"' in content


def test_ct_grants_sql_dynamic_pk_only():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15350")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    sql_text = generate_ct_grants_sql(client, tables[:1], principal_placeholder="<KEEP_USER_ID>")

    assert "DECLARE @principal SYSNAME = N'<KEEP_USER_ID>';" in sql_text
    assert "CREATE USER [" in sql_text
    assert "FOR LOGIN [" in sql_text
    assert "SKIP (no PK / use CDC grants)" in sql_text
    assert "GRANT VIEW CHANGE TRACKING ON SCHEMA::" in sql_text
    assert "GRANT SELECT, VIEW CHANGE TRACKING ON" in sql_text


def test_cdc_grants_sql_non_pk_only():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15350")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    sql_text = generate_cdc_grants_sql(client, tables[:1], principal_placeholder="<KEEP_USER_ID>")

    assert "DECLARE @principal SYSNAME = N'<KEEP_USER_ID>';" in sql_text
    assert "CREATE USER [" in sql_text
    assert "FOR LOGIN [" in sql_text
    assert "SKIP (has PK / use CT grants)" in sql_text
    assert "QUOTENAME(N'cdc')" in sql_text
    assert "@cdc_change_table = @capture_instance + N'_CT'" in sql_text


def test_table_pk_ct_status_sql_lists_active_tables():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15447")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    sql_text = generate_table_pk_ct_status_sql(client, tables)

    assert "active tables PK + CT status check" in sql_text
    assert "has_pk" in sql_text
    assert "ct_enabled" in sql_text
    assert "pk_ct_status" in sql_text
    assert "CT_NOT_ENABLED" in sql_text
    assert tables[0].table_nm in sql_text
    assert f"INSERT INTO #table_list (table_name) VALUES" in sql_text


def test_write_source_replication_sql_writes_four_files(tmp_path):
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15350")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    paths = write_source_replication_sql(client, tables, tmp_path)
    assert len(paths) == 4
    assert paths[0].endswith("_enable_ct_or_cdc.sql")
    assert paths[1].endswith("_grant_ct_access.sql")
    assert paths[2].endswith("_grant_cdc_access.sql")
    assert paths[3].endswith("_active_tables_pk_ct_status.sql")
