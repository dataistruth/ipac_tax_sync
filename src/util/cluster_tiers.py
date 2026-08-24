"""Map client_size to cluster tiers and format pipeline cluster YAML."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from util.bundle_config import (
    LAKEFLOW_SINGLE_USER_VAR_REF,
    PIPELINE_CLUSTER_NUM_WORKERS_VAR_REF,
    PIPELINE_DATA_SECURITY_MODE,
    PIPELINE_SPARK_VERSION_VAR_REF,
)
from util.models import ClientSize, ClusterTierName

if TYPE_CHECKING:
    from util.models import ClientEntry, ClusterTier

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

PIPELINE_DRIVER_NODE_D8 = "Standard_D8s_v3"
PIPELINE_WORKER_NODE_D16 = "Standard_D16s_v3"


@dataclass(frozen=True)
class PipelineClusterSpec:
    """Mixed-node ingest pipeline cluster (driver + workers)."""

    driver_node_type_id: str
    worker_node_type_id: str
    num_workers: int
    description: str = ""

    @property
    def summary(self) -> str:
        return (
            f"{self.driver_node_type_id} driver + "
            f"{self.num_workers} x {self.worker_node_type_id} worker(s)"
        )


# Default for small/medium/large (count split) and large recon types 2+.
DEFAULT_PIPELINE_CLUSTER = PipelineClusterSpec(
    driver_node_type_id=PIPELINE_DRIVER_NODE_D8,
    worker_node_type_id=PIPELINE_WORKER_NODE_D16,
    num_workers=1,
    description="D8 driver + 1 D16 worker",
)

LARGE_RECON_TYPE_1_PIPELINE_CLUSTER = PipelineClusterSpec(
    driver_node_type_id=PIPELINE_DRIVER_NODE_D8,
    worker_node_type_id=PIPELINE_WORKER_NODE_D16,
    num_workers=3,
    description="D8 driver + 3 D16 workers (large, recon_type 1)",
)


def expected_job_tier_for_size(client_size: ClientSize) -> ClusterTierName:
    return CLIENT_SIZE_TO_JOB_TIER[client_size]


def expected_serverless_tier_for_size(client_size: ClientSize) -> ClusterTierName:
    return CLIENT_SIZE_TO_SERVERLESS_TIER[client_size]


def resolve_job_tier_for_client(client: ClientEntry) -> ClusterTierName:
    return expected_job_tier_for_size(client.client_size)


def resolve_pipeline_cluster_spec(
    client: ClientEntry,
    batch: list[EffectiveTable] | None,
    split_mode: PipelineSplitMode,
) -> PipelineClusterSpec:
    if client.client_size == "large" and split_mode == "recon" and batch:
        recon_type = int(batch[0].recon_type)
        if recon_type == 1:
            return LARGE_RECON_TYPE_1_PIPELINE_CLUSTER
    return DEFAULT_PIPELINE_CLUSTER


def pipeline_cluster_note(
    client: ClientEntry,
    split_mode: PipelineSplitMode,
) -> str:
    if client.client_size == "large" and split_mode == "recon":
        return (
            "# pipeline cluster: large + split=recon → "
            "recon_type_1=D8+3xD16; other recon types=D8+1xD16"
        )
    return f"# pipeline cluster: {DEFAULT_PIPELINE_CLUSTER.summary}"


def _single_user_access_mode_lines(
    prefix: str,
    *,
    single_user_name_ref: str | None = LAKEFLOW_SINGLE_USER_VAR_REF,
) -> list[str]:
    """SINGLE_USER access mode + single_user_name (SP application ID or user email)."""
    if not single_user_name_ref:
        return []
    return [
        f"{prefix}data_security_mode: {PIPELINE_DATA_SECURITY_MODE}",
        f"{prefix}single_user_name: {single_user_name_ref}",
    ]


def format_mixed_node_cluster_spec_lines(
    spec: PipelineClusterSpec,
    *,
    spark_version_ref: str = PIPELINE_SPARK_VERSION_VAR_REF,
    single_user_name_ref: str | None = LAKEFLOW_SINGLE_USER_VAR_REF,
    indent: str = "          ",
    policy_id: str = "",
) -> list[str]:
    """Multi-node pipeline cluster: separate driver and worker node types."""
    if spec.num_workers < 1:
        raise ValueError("pipeline num_workers must be >= 1 for mixed-node clusters")
    prefix = indent
    lines = [
        f"{prefix}driver_node_type_id: {spec.driver_node_type_id}",
        f"{prefix}node_type_id: {spec.worker_node_type_id}",
        f"{prefix}spark_version: {spark_version_ref}",
        f"{prefix}num_workers: {spec.num_workers}",
    ]
    lines.extend(
        _single_user_access_mode_lines(
            prefix,
            single_user_name_ref=single_user_name_ref,
        )
    )
    if policy_id:
        lines.append(f"{prefix}policy_id: {policy_id}")
        lines.append(f"{prefix}apply_policy_default_values: true")
    return lines


def format_pipeline_cluster_lines(
    spec: PipelineClusterSpec,
    *,
    policy_id: str = "",
) -> list[str]:
    """Pipeline cluster block: mixed driver/worker nodes (Dedicated SP)."""
    lines = ["      clusters:", "        - label: default"]
    lines.extend(
        format_mixed_node_cluster_spec_lines(spec, indent="          ", policy_id=policy_id)
    )
    return lines


def format_job_cluster_spec_lines(
    tier: ClusterTier,
    *,
    spark_version_ref: str = PIPELINE_SPARK_VERSION_VAR_REF,
    num_workers_ref: str = PIPELINE_CLUSTER_NUM_WORKERS_VAR_REF,
    single_user_name_ref: str | None = LAKEFLOW_SINGLE_USER_VAR_REF,
    indent: str = "          ",
    include_single_node_custom_tag: bool = True,
    runtime_engine: str | None = None,
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
        _single_user_access_mode_lines(
            prefix,
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
    if runtime_engine:
        lines.append(f"{prefix}runtime_engine: {runtime_engine}")
    if tier.policy_id:
        lines.append(f"{prefix}policy_id: {tier.policy_id}")
        lines.append(f"{prefix}apply_policy_default_values: true")
    return lines
