"""Map client_size to cluster tiers and format pipeline cluster YAML."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from util.bundle_config import (
    LAKEFLOW_SINGLE_USER_VAR_REF,
    PIPELINE_CLUSTER_NUM_WORKERS_VAR_REF,
    PIPELINE_DATA_SECURITY_MODE_VAR_REF,
    PIPELINE_SPARK_VERSION_VAR_REF,
)
from util.models import ClientSize, ClusterTierName

if TYPE_CHECKING:
    from util.models import ClientEntry, ClusterConfig, ClusterTier, EffectiveTable

PipelineSplitMode = Literal["count", "recon"]

CLIENT_SIZE_TO_JOB_TIER: dict[ClientSize, ClusterTierName] = {
    "small": "j1",
    "medium": "j2",
    "large": "j3",
}

CLIENT_SIZE_TO_SERVERLESS_TIER: dict[ClientSize, ClusterTierName] = {
    "small": "s1",
    "medium": "s2",
    "large": "s3",
}

JOB_TIER_KEYS: tuple[ClusterTierName, ...] = ("j1", "j2", "j3")
SERVERLESS_TIER_KEYS: tuple[ClusterTierName, ...] = ("s1", "s2", "s3")

# Large client + --split recon: recon_type 1 → D64, 2 → D32, 3+ → D16 (single-node).
LARGE_CLIENT_RECON_TYPE_TO_TIER: dict[int, ClusterTierName] = {
    1: "j3",
    2: "j2",
    3: "j1",
}

# Default ingest pipeline tier for all client sizes unless large + split=recon.
DEFAULT_PIPELINE_TIER: ClusterTierName = "j1"


def expected_job_tier_for_size(client_size: ClientSize) -> ClusterTierName:
    return CLIENT_SIZE_TO_JOB_TIER[client_size]


def expected_serverless_tier_for_size(client_size: ClientSize) -> ClusterTierName:
    return CLIENT_SIZE_TO_SERVERLESS_TIER[client_size]


def resolve_job_tier_for_client(client: ClientEntry) -> ClusterTierName:
    return expected_job_tier_for_size(client.client_size)


def resolve_pipeline_tier_key(
    client: ClientEntry,
    batch: list[EffectiveTable] | None,
    split_mode: PipelineSplitMode,
) -> ClusterTierName:
    if client.client_size == "large" and split_mode == "recon" and batch:
        recon_type = int(batch[0].recon_type)
        return LARGE_CLIENT_RECON_TYPE_TO_TIER.get(recon_type, DEFAULT_PIPELINE_TIER)
    return DEFAULT_PIPELINE_TIER


def tier_for_pipeline_batch(
    client: ClientEntry,
    cluster_config: ClusterConfig,
    batch: list[EffectiveTable],
    split_mode: PipelineSplitMode,
) -> ClusterTier | None:
    tier_key = resolve_pipeline_tier_key(client, batch, split_mode)
    return cluster_config.tiers.get(tier_key)


def _dedicated_access_mode_lines(
    prefix: str,
    *,
    data_security_mode_ref: str = PIPELINE_DATA_SECURITY_MODE_VAR_REF,
    single_user_name_ref: str | None = LAKEFLOW_SINGLE_USER_VAR_REF,
) -> list[str]:
    """Dedicated access (UI) = DATA_SECURITY_MODE_DEDICATED + single_user_name (SP client ID)."""
    if not single_user_name_ref:
        return []
    return [
        f"{prefix}data_security_mode: {data_security_mode_ref}",
        f"{prefix}single_user_name: {single_user_name_ref}",
    ]


def format_job_cluster_spec_lines(
    tier: ClusterTier,
    *,
    spark_version_ref: str = PIPELINE_SPARK_VERSION_VAR_REF,
    num_workers_ref: str = PIPELINE_CLUSTER_NUM_WORKERS_VAR_REF,
    data_security_mode_ref: str = PIPELINE_DATA_SECURITY_MODE_VAR_REF,
    single_user_name_ref: str | None = LAKEFLOW_SINGLE_USER_VAR_REF,
    indent: str = "          ",
    include_single_node_custom_tag: bool = True,
) -> list[str]:
    """Single-node cluster fields from a job tier (j1/j2/j3) in cluster_config.json."""
    if tier.serverless:
        raise ValueError(f"tier {tier.label} is serverless; use job tiers j1–j3 for clusters")
    if not tier.node_type_id or tier.local_cores <= 0:
        raise ValueError(
            f"tier {tier.label} needs node_type_id and local_cores in cluster_config.json"
        )

    prefix = indent
    conf_indent = indent + "  "
    lines = [
        f"{prefix}node_type_id: {tier.node_type_id}",
        f"{prefix}spark_version: {spark_version_ref}",
        f"{prefix}num_workers: {num_workers_ref}",
        f"{prefix}spark_conf:",
        f"{conf_indent}spark.databricks.cluster.profile: singleNode",
        f"{conf_indent}spark.master: local[{tier.local_cores}]",
    ]
    lines.extend(
        _dedicated_access_mode_lines(
            prefix,
            data_security_mode_ref=data_security_mode_ref,
            single_user_name_ref=single_user_name_ref,
        )
    )
    if include_single_node_custom_tag:
        lines.extend(
            [
                f"{prefix}custom_tags:",
                f"{conf_indent}ResourceClass: SingleNode",
            ]
        )
    return lines


def format_pipeline_cluster_lines(tier: ClusterTier) -> list[str]:
    """Pipeline cluster block: single-node job tier (Dedicated SP)."""
    lines = ["      clusters:", "        - label: default"]
    lines.extend(format_job_cluster_spec_lines(tier, indent="          "))
    if tier.policy_id:
        lines.append(f"          policy_id: {tier.policy_id}")
        lines.append("          apply_policy_default_values: true")
    return lines
