"""Tests for ipac_delta_sync config/common model."""

from util.bundle_config import resolve_uc_catalog, uc_catalog_var_ref
from util.config_loader import (
    get_client,
    load_client_overrides,
    load_common_tables,
    load_client_registry,
)
from util.pipeline_generator import (
    chunk_tables,
    generate_client_pipelines_yaml,
    generate_lakeflow_pipeline_yaml,
    pipeline_resource_key,
    _format_object_lines,
)
from util.resolver import resolve_effective_tables

UC_REF = uc_catalog_var_ref()


def test_client_a_resolves_ten_common_tables():
    catalog = load_common_tables()
    client = get_client("client_a")
    overrides = load_client_overrides("client_a")
    tables = resolve_effective_tables(client, catalog, overrides)

    names = {t.table_nm for t in tables}
    assert "partners" in names
    assert "gl_transactions" in names
    assert "fact_gl_line_detail" not in names
    assert "fact_k1_allocation_detail" not in names
    assert len(tables) == 10


def test_client_b_extra_only():
    catalog = load_common_tables()
    client = get_client("client_b")
    overrides = load_client_overrides("client_b")
    tables = resolve_effective_tables(client, catalog, overrides)

    assert len(tables) == 1
    assert tables[0].table_nm == "CYAdjustmentInput"
    assert tables[0].source == "extra"


def test_active_client_count():
    registry = load_client_registry()
    active = [c for c in registry.clients if c.is_active]
    assert len(active) == 2


def test_resolve_uc_catalog_from_databricks_yml():
    assert resolve_uc_catalog() == "ipac_tax_synch"
    assert resolve_uc_catalog(target="dev") == "ipac_tax_synch"


def test_scd_type_on_resolved_tables():
    catalog = load_common_tables()
    client = get_client("client_a")
    overrides = load_client_overrides("client_a")
    tables = resolve_effective_tables(client, catalog, overrides)

    by_name = {t.table_nm: t for t in tables}
    assert by_name["tax_adjustments"].scd_type == 2
    assert by_name["partners"].scd_type == 1


def test_no_cluster_when_lq_key_empty():
    catalog = load_common_tables()
    client = get_client("client_a")
    overrides = load_client_overrides("client_a")
    tables = resolve_effective_tables(client, catalog, overrides)

    tracker = next(t for t in tables if t.table_nm == "document_tracker")
    assert not tracker.has_cluster_by
    lines = _format_object_lines(tracker, client, UC_REF)
    assert "clustering_columns" not in "\n".join(lines)


def test_cluster_when_lq_key_defined():
    catalog = load_common_tables()
    client = get_client("client_a")
    overrides = load_client_overrides("client_a")
    tables = resolve_effective_tables(client, catalog, overrides)

    partners = next(t for t in tables if t.table_nm == "partners")
    lines = _format_object_lines(partners, client, UC_REF)
    assert "clustering_columns: [partner_id]" in "\n".join(lines)


def test_scd_type_2_in_table_configuration():
    catalog = load_common_tables()
    client = get_client("client_a")
    overrides = load_client_overrides("client_a")
    tables = resolve_effective_tables(client, catalog, overrides)

    adjustments = next(t for t in tables if t.table_nm == "tax_adjustments")
    lines = _format_object_lines(adjustments, client, UC_REF)
    joined = "\n".join(lines)
    assert "scd_type: 2" in joined
    assert "clustering_columns: [entity_id, period_id]" in joined


def test_raw_and_staging_share_client_nm_raw_schema():
    catalog = load_common_tables()
    client = get_client("client_a")
    overrides = load_client_overrides("client_a")
    tables = resolve_effective_tables(client, catalog, overrides)
    yaml_text = generate_lakeflow_pipeline_yaml(
        client, tables, uc_catalog_ref=UC_REF, resolved_uc_catalog="ipac_tax_synch"
    )

    assert client.raw_schema() == "client_a_raw"
    assert "schema: client_a_raw" in yaml_text
    assert "schema_name: client_a_raw" in yaml_text
    assert "destination_schema: 'client_a_raw'" in yaml_text
    assert "catalog: ${var.uc_catalog}" in yaml_text
    assert "catalog_name: ${var.uc_catalog}" in yaml_text
    assert "destination_catalog: ${var.uc_catalog}" in yaml_text
    assert "volume_name:" not in yaml_text


def test_explicit_volume_name_in_staging():
    catalog = load_common_tables()
    client = get_client("client_a")
    client.volume = "lakeflow_staging"
    overrides = load_client_overrides("client_a")
    tables = resolve_effective_tables(client, catalog, overrides)
    yaml_text = generate_lakeflow_pipeline_yaml(client, tables, uc_catalog_ref=UC_REF)

    assert "volume_name: lakeflow_staging" in yaml_text
    assert "schema_name: client_a_raw" in yaml_text


def test_chunk_tables_batch_sizes():
    items = list(range(12))
    batches = chunk_tables(items, 5)
    assert len(batches) == 3
    assert len(batches[0]) == 5
    assert len(batches[1]) == 5
    assert len(batches[2]) == 2

    ten_batches = chunk_tables(list(range(10)), 5)
    assert len(ten_batches) == 2
    assert len(ten_batches[0]) == 5
    assert len(ten_batches[1]) == 5


def test_pipeline_resource_key():
    assert pipeline_resource_key("client_a", 1) == "p_client_a_1"
    assert pipeline_resource_key("client_a", 3) == "p_client_a_3"


def test_client_a_splits_into_two_pipelines_with_batch_five():
    catalog = load_common_tables()
    client = get_client("client_a")
    overrides = load_client_overrides("client_a")
    tables = resolve_effective_tables(client, catalog, overrides)
    yaml_text = generate_client_pipelines_yaml(
        client, tables, uc_catalog_ref=UC_REF, num_of_tables_in_pipeline=5
    )

    assert "p_client_a_1:" in yaml_text
    assert "p_client_a_2:" in yaml_text
    assert "name: p_client_a_1" in yaml_text
    assert "name: p_client_a_2" in yaml_text
    assert "across 2 pipeline(s) [5, 5]" in yaml_text
    assert "client_a_lakeflow_cdc" not in yaml_text


def test_twelve_tables_split_three_pipelines():
    catalog = load_common_tables()
    client = get_client("client_a")
    overrides = load_client_overrides("client_a")
    tables = resolve_effective_tables(client, catalog, overrides)
    # Simulate 12 tables by duplicating first two entries with new names
    extra = tables[:2]
    for i, t in enumerate(extra):
        clone = t.model_copy()
        clone.table_nm = f"synthetic_table_{i}"
        tables.append(clone)

    yaml_text = generate_client_pipelines_yaml(
        client, tables, uc_catalog_ref=UC_REF, num_of_tables_in_pipeline=5
    )
    assert "across 3 pipeline(s) [5, 5, 2]" in yaml_text
    assert "p_client_a_3:" in yaml_text


def test_generate_yaml_continuous_lakeflow():
    catalog = load_common_tables()
    client = get_client("client_a")
    overrides = load_client_overrides("client_a")
    tables = resolve_effective_tables(client, catalog, overrides)
    yaml_text = generate_lakeflow_pipeline_yaml(
        client, tables, uc_catalog_ref=UC_REF, num_of_tables_in_pipeline=5
    )

    assert "p_client_a_1" in yaml_text
    assert "p_client_a_2" in yaml_text
    assert "continuous: true" in yaml_text
    assert "MANAGED_INGESTION" in yaml_text
    assert "connector_type: CDC" in yaml_text
    assert "sql_server_lakeflow_connect" in yaml_text
    assert "source_table: 'partners'" in yaml_text
    assert "clustering_columns: [entity_id, period_id]" in yaml_text
    assert "scd_type: 2" in yaml_text
    assert "${var.uc_catalog}" in yaml_text

    tracker_block_start = yaml_text.index("source_table: 'document_tracker'")
    tracker_block = yaml_text[tracker_block_start:tracker_block_start + 400]
    assert "clustering_columns" not in tracker_block
