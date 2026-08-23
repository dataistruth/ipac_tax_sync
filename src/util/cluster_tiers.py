"""Map client_size to cluster tiers and format pipeline cluster YAML."""

from __future__ import annotations

from typing import TYPE_CHECKING

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


def expected_job_tier_for_size(client_size: ClientSize) -> ClusterTierName:
    return CLIENT_SIZE_TO_JOB_TIER[client_size]


def expected_serverless_tier_for_size(client_size: ClientSize) -> ClusterTierName:
    return CLIENT_SIZE_TO_SERVERLESS_TIER[client_size]


def resolve_job_tier_for_client(client: ClientEntry) -> ClusterTierName:
    """Job cluster tier for Lakeflow Connect: small→j1, medium→j2, large→j3."""
    return expected_job_tier_for_size(client.client_size)


def format_pipeline_cluster_lines(
    tier: ClusterTier,
    spark_version_ref: str,
    *,
    instance_pool_ref: str | None = None,
    node_type_ref: str | None = None,
    num_workers_ref: str = "${var.pipeline_cluster_num_workers}",
    cluster_spark_master_ref: str = "${var.cluster_spark_master}",
    data_security_mode_ref: str = "${var.cluster_data_security_mode}",
    dedicated_principal_ref: str = "${var.dedicated_compute_principal}",
) -> list[str]:
    """YAML lines for classic (non-serverless) pipeline cluster block."""
    lines = [
        "      clusters:",
        "        - label: default",
    ]

    if instance_pool_ref:
        lines.append(f"          instance_pool_id: {instance_pool_ref}")
        lines.append(f"          driver_instance_pool_id: {instance_pool_ref}")
    elif node_type_ref:
        lines.append(f"          node_type_id: {node_type_ref}")
        if tier.driver_node_type_id:
            lines.append(f"          driver_node_type_id: {tier.driver_node_type_id}")
    else:
        raise ValueError("format_pipeline_cluster_lines requires instance_pool_ref or node_type_ref")

    lines.extend(
        [
            f"          spark_version: {spark_version_ref}",
            f"          num_workers: {num_workers_ref}",
            "          spark_conf:",
            "            spark.databricks.cluster.profile: singleNode",
            f"            spark.master: {cluster_spark_master_ref}",
            "          custom_tags:",
            "            ResourceClass: SingleNode",
            f"          data_security_mode: {data_security_mode_ref}",
            f"          single_user_name: {dedicated_principal_ref}",
        ]
    )
    if tier.policy_id:
        lines.append(f"          policy_id: {tier.policy_id}")
    return lines
