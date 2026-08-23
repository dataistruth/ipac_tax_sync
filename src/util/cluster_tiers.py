"""Map client_size to cluster tiers and format pipeline cluster YAML."""

from __future__ import annotations

from typing import TYPE_CHECKING

from util.bundle_config import (
    PIPELINE_CLUSTER_NUM_WORKERS_VAR_REF,
    PIPELINE_SPARK_VERSION_VAR_REF,
)
from util.models import ClientSize, ClusterTierName

if TYPE_CHECKING:
    from util.models import ClientEntry, ClusterTier

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


def expected_job_tier_for_size(client_size: ClientSize) -> ClusterTierName:
    return CLIENT_SIZE_TO_JOB_TIER[client_size]


def expected_serverless_tier_for_size(client_size: ClientSize) -> ClusterTierName:
    return CLIENT_SIZE_TO_SERVERLESS_TIER[client_size]


def resolve_job_tier_for_client(client: ClientEntry) -> ClusterTierName:
    return expected_job_tier_for_size(client.client_size)


def format_job_cluster_spec_lines(
    tier: ClusterTier,
    *,
    spark_version_ref: str = PIPELINE_SPARK_VERSION_VAR_REF,
    num_workers_ref: str = PIPELINE_CLUSTER_NUM_WORKERS_VAR_REF,
    indent: str = "          ",
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
    return [
        f"{prefix}node_type_id: {tier.node_type_id}",
        f"{prefix}spark_version: {spark_version_ref}",
        f"{prefix}num_workers: {num_workers_ref}",
        f"{prefix}spark_conf:",
        f"{conf_indent}spark.databricks.cluster.profile: singleNode",
        f"{conf_indent}spark.master: local[{tier.local_cores}]",
        f"{prefix}custom_tags:",
        f"{conf_indent}ResourceClass: SingleNode",
    ]


def format_pipeline_cluster_lines(tier: ClusterTier) -> list[str]:
    """Pipeline cluster block: tier from client_size → j1/j2/j3."""
    lines = ["      clusters:", "        - label: default"]
    lines.extend(
        format_job_cluster_spec_lines(tier, indent="          ")
    )
    if tier.policy_id:
        lines.append(f"          policy_id: {tier.policy_id}")
    return lines
