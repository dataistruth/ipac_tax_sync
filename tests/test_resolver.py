"""Tests for ipac_delta_sync config/common model."""

from util.bundle_config import (
    LAKEFLOW_INSTANCE_POOL_ID_REF,
    PIPELINE_CLUSTER_AUTOSCALE_MIN_VAR_REF,
    PIPELINE_CLUSTER_AUTOSCALE_MAX_VAR_REF,
    pipeline_cluster_num_workers_var_ref,
    pipeline_tag_var_ref,
    pipeline_max_update_retry_attempts_var_ref,
    pipeline_spark_version_var_ref,
    resolve_dest_schema_suffix,
    resolve_num_of_tables_in_pipeline,
    resolve_recon_cluster_tier,
    resolve_uc_catalog,
    uc_catalog_var_ref,
)
from util.cluster_tiers import expected_job_tier_for_size
from util.config_loader import (
    get_client,
    load_client_overrides,
    load_client_registry,
    load_cluster_config,
    load_common_tables,
)
from util.pipeline_generator import (
    chunk_tables,
    generate_client_pipelines_yaml,
)
from util.pipeline_registry import write_pipeline_name_registry
from util.resolver import resolve_effective_tables
from util.schema_generator import generate_schema_resource_yaml, schema_resource_name_ref
from util.sql_generator import (
    generate_enable_ct_sql,
    generate_table_pk_ct_status_sql,
    generate_cdc_grants_sql,
    generate_ct_grants_sql,
    write_source_replication_sql,
)

UC_REF = uc_catalog_var_ref()
PIPELINE_TAG_REF = pipeline_tag_var_ref()
RETRY_REF = pipeline_max_update_retry_attempts_var_ref()
SPARK_REF = pipeline_spark_version_var_ref()
NUM_WORKERS_REF = pipeline_cluster_num_workers_var_ref()
POOL_REF = LAKEFLOW_INSTANCE_POOL_ID_REF
AUTOSCALE_MIN_REF = PIPELINE_CLUSTER_AUTOSCALE_MIN_VAR_REF
AUTOSCALE_MAX_REF = PIPELINE_CLUSTER_AUTOSCALE_MAX_VAR_REF


def _active_clients():
    registry = load_client_registry()
    return [c for c in registry.clients if c.is_active]


def test_active_client_count():
    assert len(_active_clients()) == 3


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


def test_snapshot_in_table_name_sets_recon_type_2():
    from util.models import CommonTable

    assert CommonTable(table_nm="K1Input_Snapshot", recon_type=1).recon_type == 2
    assert CommonTable(table_nm="TrialBalanceAdjustments_SnapShot", recon_type=1).recon_type == 2
    assert CommonTable(table_nm="Entity", recon_type=1).recon_type == 1

    catalog = load_common_tables()
    snapshots = [t for t in catalog.tables if "snapshot" in t.table_nm.casefold()]
    assert len(snapshots) >= 40
    assert all(t.recon_type == 2 for t in snapshots)


def test_resolve_effective_tables_for_first_client():
    catalog = load_common_tables()
    client = _active_clients()[0]
    overrides = load_client_overrides(client.client_nm)
    tables = resolve_effective_tables(client, catalog, overrides)

    assert len(tables) > 0
    assert len(tables) < len([t for t in catalog.tables if t.is_active])
    assert all(t.recon_type in (1, 2, 3) for t in tables)
    assert all(t.src_schema == "dbo" for t in tables)


def test_dest_schema_suffix_default_empty():
    assert resolve_dest_schema_suffix() == "_poc_1"


def test_generate_yaml_uses_per_client_destination_schema_with_suffix():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15350")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    dest_schema = client.raw_schema("poc_1")
    yaml_text = generate_client_pipelines_yaml(
        client,
        tables,
        uc_catalog_ref=UC_REF,
        resolved_uc_catalog=resolve_uc_catalog(),
        num_of_tables_in_pipeline=resolve_num_of_tables_in_pipeline(),
        dest_schema_suffix="poc_1",
    )

    assert f"schema: {schema_resource_name_ref(dest_schema)}" in yaml_text
    assert "tags:" not in yaml_text
    assert f"destination_schema: ${{resources.schemas.schema_ipc_2025_dev7_15350poc_1.name}}" in yaml_text
    assert dest_schema == "iPC_2025_Dev7_15350poc_1"
    assert "data_staging_options:" not in yaml_text
    assert "serverless: false" in yaml_text
    assert "continuous: true" in yaml_text
    assert "development: false" in yaml_text
    assert f"pipelines.numUpdateRetryAttempts: {RETRY_REF}" in yaml_text
    assert "clusters:" in yaml_text
    assert f"instance_pool_id: {POOL_REF}" in yaml_text
    assert "data_security_mode: ${var.pipeline_data_security_mode}" in yaml_text
    assert "single_user_name: ${var.lakeflow_single_user}" in yaml_text
    assert f"min_workers: {AUTOSCALE_MIN_REF}" in yaml_text
    assert f"max_workers: {AUTOSCALE_MAX_REF}" in yaml_text
    depends_on_block = yaml_text.split("depends_on:", 1)[1].split("pipeline_type:", 1)[0]
    assert "instance_pools" not in depends_on_block
    assert "depends_on:" in yaml_text
    assert "resources.schemas.schema_ipac_metadata" in yaml_text
    assert f"spark_version: {SPARK_REF}" in yaml_text
    assert "node_type_id:" not in yaml_text
    assert "num_workers:" not in yaml_text
    assert "spark.master:" not in yaml_text
    assert "ResourceClass: SingleNode" not in yaml_text
    assert "autoscale:" in yaml_text
    assert "event_log:" not in yaml_text
    for table in tables:
        assert f"destination_table: '{table.table_nm}'" in yaml_text
        idx = yaml_text.index(f"destination_table: '{table.table_nm}'")
        snippet = yaml_text[idx:idx + 200]
        assert "scd_type: SCD_TYPE_1" in snippet


def test_generate_yaml_uses_j3_for_large_client():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15350").model_copy(
        update={"client_size": "large", "cluster_tier": "j3"}
    )
    cluster_cfg = load_cluster_config()
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    yaml_text = generate_client_pipelines_yaml(
        client,
        tables[:1],
        cluster_config=cluster_cfg,
        uc_catalog_ref=UC_REF,
        num_of_tables_in_pipeline=1,
        dest_schema_suffix="poc_1",
        use_instance_pool=False,
    )
    assert "node_type_id: Standard_D64s_v3" in yaml_text
    assert "spark.master: local[64]" in yaml_text
    assert "instance_pool_id:" not in yaml_text


def test_generate_yaml_uses_j1_for_small_client():
    client = get_client("iPC_2025_Dev7_15350").model_copy(
        update={"client_size": "small", "cluster_tier": "j1"}
    )
    catalog = load_common_tables()
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    yaml_text = generate_client_pipelines_yaml(
        client,
        tables[:1],
        uc_catalog_ref=UC_REF,
        num_of_tables_in_pipeline=1,
        dest_schema_suffix="poc_1",
        use_instance_pool=False,
    )
    assert "node_type_id: Standard_D16s_v3" in yaml_text
    assert "spark.master: local[16]" in yaml_text


def test_generate_yaml_uses_suffix_when_provided():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15447")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    dest_schema = client.raw_schema("_raw")
    yaml_text = generate_client_pipelines_yaml(
        client,
        tables,
        uc_catalog_ref=UC_REF,
        dest_schema_suffix="_raw",
    )

    assert f"schema: {schema_resource_name_ref(dest_schema)}" in yaml_text
    assert "tags:" not in yaml_text
    assert f"destination_schema: ${{resources.schemas.schema_ipc_2025_dev7_15447_raw.name}}" in yaml_text
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
    assert "schemas:" not in yaml_text.split("pipelines:")[0]
    assert "p_iPC_2025_Dev7_15350_1:" in yaml_text
    assert "resources.schemas.schema_ipac_metadata" in yaml_text
    assert "event_log:" not in yaml_text


def test_enable_ct_sql_pk_only_no_cdc():
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15350")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    sql_text = generate_enable_ct_sql(client, tables[:1])

    assert "ENABLE CHANGE_TRACKING" in sql_text
    assert "sp_cdc_enable_table" not in sql_text
    assert "sp_cdc_enable_db" not in sql_text
    assert "SKIP (no PK" in sql_text
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


def test_load_client_overrides_allows_client_nm_case_mismatch(tmp_path):
    override_dir = tmp_path / "overrides"
    override_dir.mkdir()
    override_file = override_dir / "iPC_2025_Dev7_15447.json"
    override_file.write_text(
        '{"client_nm": "iPC_2025_Dev7_15447", "ignore": [], "extra": []}',
        encoding="utf-8",
    )

    overrides = load_client_overrides(
        "iPC_2025_DEV7_15447",
        path=override_file,
    )
    assert overrides is not None
    assert overrides.client_nm == "iPC_2025_Dev7_15447"


def test_write_source_replication_sql_writes_four_files(tmp_path):
    catalog = load_common_tables()
    client = get_client("iPC_2025_Dev7_15350")
    tables = resolve_effective_tables(client, catalog, load_client_overrides(client.client_nm))
    paths = write_source_replication_sql(client, tables, tmp_path)
    assert len(paths) == 4
    assert paths[0].endswith("_enable_ct.sql")
    assert paths[1].endswith("_grant_ct_access.sql")
    assert paths[2].endswith("_grant_cdc_access.sql")
    assert paths[3].endswith("_active_tables_pk_ct_status.sql")


def test_write_ipac_metadata_schema_resource_yaml(tmp_path):
    from pathlib import Path

    from util.schema_generator import write_ipac_metadata_schema_resource_yaml

    path = write_ipac_metadata_schema_resource_yaml(tmp_path)
    text = Path(path).read_text(encoding="utf-8")
    assert "schema_ipac_metadata:" in text
    assert "name: ${var.ipac_metadata_schema}" in text
    assert "catalog_name: ${var.uc_catalog}" in text
    assert path.endswith("ipac_metadata_schema.yml")

