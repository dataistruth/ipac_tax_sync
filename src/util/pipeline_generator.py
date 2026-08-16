"""Generate continuous Lakeflow Connect pipeline YAML from resolved configs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from common.ops.recon_store import ingest_event_log_table_name
from util.bundle_config import (
    METADATA_SCHEMA_CATALOG_REF,
    METADATA_SCHEMA_NAME_REF,
    PIPELINE_CLUSTER_NODE_TYPE_VAR_REF,
    PIPELINE_MAX_UPDATE_RETRY_ATTEMPTS_VAR_REF,
    PIPELINE_SPARK_VERSION_VAR_REF,
    PIPELINE_TAG_VAR_REF,
    UC_CATALOG_VAR_REF,
    UC_LKF_STAGING_SCHEMA_VAR_REF,
)
from util.cluster_tiers import expected_job_tier_for_size, format_pipeline_cluster_lines
from util.schema_generator import schema_resource_key, schema_resource_name_ref

if TYPE_CHECKING:
    from util.models import ClientEntry, ClusterConfig, ClusterTier, EffectiveTable


def pipeline_resource_key(client_nm: str, serial: int) -> str:
    """Bundle pipeline key/name: p_<client_nm>_<serial>."""
    return f"p_{client_nm}_{serial}"


def chunk_tables(tables: list[EffectiveTable], batch_size: int) -> list[list[EffectiveTable]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [tables[i : i + batch_size] for i in range(0, len(tables), batch_size)]


def _parse_lq_key(raw: str | None) -> list[str]:
    if not raw or not str(raw).strip():
        return []
    return [c.strip() for c in str(raw).split(",") if c.strip()]


def _tier_for_client(client: ClientEntry, cluster_config: ClusterConfig | None) -> ClusterTier | None:
    if cluster_config is None:
        from util.config_loader import load_cluster_config

        cluster_config = load_cluster_config()
    tier_key = expected_job_tier_for_size(client.client_size)
    return cluster_config.tiers.get(tier_key)


def _yaml_scd_type(scd_type: int) -> str:
    """Lakeflow ingestion table_configuration scd enum (not numeric 1/2)."""
    if scd_type == 2:
        return "SCD_TYPE_2"
    return "SCD_TYPE_1"


def _format_object_lines(
    table: EffectiveTable,
    client: ClientEntry,
    uc_catalog_ref: str,
    dest_schema_suffix: str,
) -> list[str]:
    raw_schema = client.raw_schema(dest_schema_suffix)
    dest_schema_ref = schema_resource_name_ref(raw_schema)
    lines = [
        "          - table:",
        f"              source_catalog: '{client.src_db_nm}'",
        f"              source_schema: '{table.src_schema}'",
        f"              source_table: '{table.table_nm}'",
        f"              destination_catalog: {uc_catalog_ref}",
        f"              destination_schema: {dest_schema_ref}",
        f"              destination_table: '{table.table_nm}'",
    ]

    clustering = _parse_lq_key(table.lq_key) if table.has_cluster_by else []
    lines.append("              table_configuration:")
    lines.append(f"                scd_type: {_yaml_scd_type(table.scd_type)}")
    if clustering:
        cols = ", ".join(clustering)
        lines.append(f"                clustering_columns: [{cols}]")
    if table.recon_type != 1:
        lines.append(f"                recon_type: {table.recon_type}")
    return lines


def _schema_depends_on(schema_name: str) -> str:
    return f"resources.schemas.{schema_resource_key(schema_name)}"


def _pipeline_resource_lines(
    client: ClientEntry,
    tables: list[EffectiveTable],
    serial: int,
    cluster_config: ClusterConfig | None,
    uc_catalog_ref: str,
    uc_lkf_staging_schema_ref: str,
    pipeline_tag_ref: str,
    dest_schema_suffix: str,
    pipeline_max_update_retry_attempts_ref: str = PIPELINE_MAX_UPDATE_RETRY_ATTEMPTS_VAR_REF,
    metadata_schema: str = "ipac_metadata",
) -> list[str]:
    if not tables:
        raise ValueError(f"Pipeline batch {serial} has no tables for {client.client_nm}")

    tier = _tier_for_client(client, cluster_config)
    pipeline_key = pipeline_resource_key(client.client_nm, serial)
    job_tier_key = expected_job_tier_for_size(client.client_size)
    meta_dep = _schema_depends_on(metadata_schema)
    client_raw_dep = _schema_depends_on(client.raw_schema(dest_schema_suffix))

    lines = [
        f"    {pipeline_key}:",
        f"      name: {pipeline_key}",
        "      depends_on:",
        f"        - {meta_dep}",
        f"        - {client_raw_dep}",
        "      pipeline_type: MANAGED_INGESTION",
        "      event_log:",
        f"        catalog: {METADATA_SCHEMA_CATALOG_REF}",
        f"        schema: {METADATA_SCHEMA_NAME_REF}",
        f"        name: {ingest_event_log_table_name(pipeline_key)}",
        "      permissions:",
        "        - level: CAN_MANAGE",
        "          group_name: ${var.grant_group}",
        "      tags:",
        f"        bundle: {pipeline_tag_ref}",
        "      channel: PREVIEW",
        "      serverless: false",
        "      continuous: true",
        "      configuration:",
        f"        pipelines.numUpdateRetryAttempts: {pipeline_max_update_retry_attempts_ref}",
        f"      catalog: {uc_catalog_ref}",
        f"      schema: {uc_lkf_staging_schema_ref}",
    ]

    if tier:
        lines.extend(
            format_pipeline_cluster_lines(
                tier,
                PIPELINE_CLUSTER_NODE_TYPE_VAR_REF,
                PIPELINE_SPARK_VERSION_VAR_REF,
            )
        )
    else:
        lines.append(f"      # cluster tier {job_tier_key}: cluster_config.json missing — add clusters block")

    lines.extend(
        [
        "      ingestion_definition:",
        f"        connection_name: {client.uc_conn_nm}",
        "        connector_type: CDC",
        "        table_configuration:",
        "          enable_auto_clustering: true",
        "        objects:",
        ]
    )

    for table in tables:
        lines.extend(_format_object_lines(table, client, uc_catalog_ref, dest_schema_suffix))

    return lines


def generate_client_pipelines_yaml(
    client: ClientEntry,
    tables: list[EffectiveTable],
    cluster_config: ClusterConfig | None = None,
    uc_catalog_ref: str = UC_CATALOG_VAR_REF,
    resolved_uc_catalog: str | None = None,
    num_of_tables_in_pipeline: int = 5,
    dest_schema_suffix: str = "",
    uc_lkf_staging_schema_ref: str = UC_LKF_STAGING_SCHEMA_VAR_REF,
    resolved_uc_lkf_staging_schema: str | None = None,
    pipeline_tag_ref: str = PIPELINE_TAG_VAR_REF,
    pipeline_max_update_retry_attempts_ref: str = PIPELINE_MAX_UPDATE_RETRY_ATTEMPTS_VAR_REF,
    metadata_schema: str = "ipac_metadata",
) -> str:
    """Generate bundle YAML with one or more pipelines split by num_of_tables_in_pipeline."""
    if not tables:
        raise ValueError(f"No tables to generate for {client.client_nm}")

    batches = chunk_tables(tables, num_of_tables_in_pipeline)
    raw_schema = client.raw_schema(dest_schema_suffix)
    catalog_comment = resolved_uc_catalog or uc_catalog_ref
    lkf_schema_comment = resolved_uc_lkf_staging_schema or uc_lkf_staging_schema_ref
    tier = _tier_for_client(client, cluster_config)

    tier_note = ""
    if tier:
        job_tier_key = expected_job_tier_for_size(client.client_size)
        tier_note = (
            f"# client_size: {client.client_size} → job tier {job_tier_key} ({tier.label}) — "
            f"{tier.description} (autoscale {tier.min_workers}-{tier.max_workers} workers)"
        )

    batch_summary = ", ".join(str(len(b)) for b in batches)
    client_schema_key = schema_resource_key(raw_schema)
    lines = [
        f"# Generated by ipac_delta_sync for client: {client.client_nm}",
        f"# {client.desc}",
        f"# uc_catalog: {catalog_comment} (from databricks.yml var.uc_catalog)",
        f"# pipeline schema: {lkf_schema_comment} (from databricks.yml var.uc_lkf_staging_schema)",
        f"# table destination_schema: {catalog_comment}.{raw_schema} "
        f"(resources.schemas.{client_schema_key})",
        f"# num_of_tables_in_pipeline: {num_of_tables_in_pipeline} "
        f"(databricks.yml var.num_of_tables_in_pipeline)",
        tier_note,
        f"# tables: {len(tables)} across {len(batches)} pipeline(s) [{batch_summary}]",
        f"# Regenerate: ipac-delta-sync generate --client {client.client_nm}",
        "resources:",
        "  pipelines:",
    ]

    for serial, batch in enumerate(batches, start=1):
        lines.extend(
            _pipeline_resource_lines(
                client,
                batch,
                serial,
                cluster_config,
                uc_catalog_ref,
                uc_lkf_staging_schema_ref,
                pipeline_tag_ref,
                dest_schema_suffix,
                pipeline_max_update_retry_attempts_ref,
                metadata_schema,
            )
        )

    return "\n".join([line for line in lines if line]) + "\n"


def write_bundle_pipeline_yaml(
    client: ClientEntry,
    tables: list[EffectiveTable],
    output_dir,
    cluster_config: ClusterConfig | None = None,
    uc_catalog_ref: str = UC_CATALOG_VAR_REF,
    resolved_uc_catalog: str | None = None,
    num_of_tables_in_pipeline: int = 5,
    dest_schema_suffix: str = "",
    uc_lkf_staging_schema_ref: str = UC_LKF_STAGING_SCHEMA_VAR_REF,
    resolved_uc_lkf_staging_schema: str | None = None,
    pipeline_tag_ref: str = PIPELINE_TAG_VAR_REF,
    pipeline_max_update_retry_attempts_ref: str = PIPELINE_MAX_UPDATE_RETRY_ATTEMPTS_VAR_REF,
    metadata_schema: str = "ipac_metadata",
) -> str:
    from pathlib import Path

    out_file = Path(output_dir) / f"{client.client_nm}_pipeline.yml"
    content = generate_client_pipelines_yaml(
        client,
        tables,
        cluster_config,
        uc_catalog_ref=uc_catalog_ref,
        resolved_uc_catalog=resolved_uc_catalog,
        num_of_tables_in_pipeline=num_of_tables_in_pipeline,
        dest_schema_suffix=dest_schema_suffix,
        uc_lkf_staging_schema_ref=uc_lkf_staging_schema_ref,
        resolved_uc_lkf_staging_schema=resolved_uc_lkf_staging_schema,
        pipeline_tag_ref=pipeline_tag_ref,
        pipeline_max_update_retry_attempts_ref=pipeline_max_update_retry_attempts_ref,
        metadata_schema=metadata_schema,
    )
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(content, encoding="utf-8")
    return str(out_file)
