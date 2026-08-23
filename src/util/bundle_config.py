"""Read shared bundle settings from databricks.yml."""

from __future__ import annotations

from pathlib import Path

import yaml

from util.models import ClusterTierName
from util.paths import project_root

UC_CATALOG_VAR_REF = "${var.uc_catalog}"
IPAC_METADATA_SCHEMA_VAR_REF = "${var.ipac_metadata_schema}"
PIPELINE_TAG_VAR_REF = "${var.pipeline_tag}"
JOB_TAG_VAR_REF = "${var.job_tag}"
PIPELINE_MAX_UPDATE_RETRY_ATTEMPTS_VAR_REF = "${var.pipeline_max_update_retry_attempts}"
PIPELINE_SPARK_VERSION_VAR_REF = "${var.pipeline_spark_version}"
PIPELINE_CLUSTER_NUM_WORKERS_VAR_REF = "${var.pipeline_cluster_num_workers}"
HEARTBEAT_JOB_ALERT_MAIL_VAR_REF = "${var.heartbeat_job_alert_mail}"
RECON_POLL_INTERVAL_SEC_VAR_REF = "${var.recon_poll_interval_sec}"
LAKEFLOW_SINGLE_USER_VAR_REF = "${var.lakeflow_single_user}"


def databricks_yml_path() -> Path:
    return project_root() / "databricks.yml"


def load_databricks_bundle_config(path: Path | None = None) -> dict:
    file_path = path or databricks_yml_path()
    if not file_path.exists():
        raise FileNotFoundError(f"databricks.yml not found: {file_path}")
    with file_path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _resolve_variable(
    name: str,
    default: str | int,
    override: str | int | None = None,
    target: str | None = None,
    databricks_yml: Path | None = None,
) -> str | int:
    if override is not None and str(override).strip():
        return override

    config = load_databricks_bundle_config(databricks_yml)
    variables = config.get("variables") or {}
    raw = variables.get(name)

    if isinstance(raw, dict):
        if target:
            targets = config.get("targets") or {}
            target_cfg = targets.get(target) or {}
            target_vars = target_cfg.get("variables") or {}
            if target_vars.get(name) is not None:
                return target_vars[name]
        if raw.get("default") is not None:
            return raw["default"]

    if raw is not None:
        return raw

    return default


def resolve_uc_catalog(
    override: str | None = None,
    target: str | None = None,
    databricks_yml: Path | None = None,
) -> str:
    value = _resolve_variable(
        "uc_catalog",
        "main",
        override=override,
        target=target,
        databricks_yml=databricks_yml,
    )
    if isinstance(value, str):
        return value.strip()
    return str(value)


def resolve_num_of_tables_in_pipeline(
    override: int | str | None = None,
    target: str | None = None,
    databricks_yml: Path | None = None,
) -> int:
    value = _resolve_variable(
        "num_of_tables_in_pipeline",
        5,
        override=override,
        target=target,
        databricks_yml=databricks_yml,
    )
    batch_size = int(value)
    if batch_size <= 0:
        raise ValueError("num_of_tables_in_pipeline must be a positive integer")
    return batch_size


def resolve_dest_schema_suffix(
    override: str | None = None,
    target: str | None = None,
    databricks_yml: Path | None = None,
) -> str:
    value = _resolve_variable(
        "dest_schema_suffix",
        "",
        override=override,
        target=target,
        databricks_yml=databricks_yml,
    )
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def resolve_ipac_metadata_schema(
    override: str | None = None,
    target: str | None = None,
    databricks_yml: Path | None = None,
) -> str:
    value = _resolve_variable(
        "ipac_metadata_schema",
        "ipac_metadata",
        override=override,
        target=target,
        databricks_yml=databricks_yml,
    )
    if isinstance(value, str):
        resolved = value.strip()
    else:
        resolved = str(value).strip()
    if not resolved:
        raise ValueError("ipac_metadata_schema must not be empty")
    return resolved


def resolve_recon_cluster_tier(
    override: str | None = None,
    target: str | None = None,
    databricks_yml: Path | None = None,
) -> ClusterTierName:
    value = _resolve_variable(
        "recon_cluster_tier",
        "j3",
        override=override,
        target=target,
        databricks_yml=databricks_yml,
    )
    tier = str(value).strip().lower()
    if tier not in ("j1", "j2", "j3"):
        raise ValueError("recon_cluster_tier must be j1, j2, or j3")
    return tier


def uc_catalog_var_ref() -> str:
    return UC_CATALOG_VAR_REF


def resolve_pipeline_tag(
    override: str | None = None,
    target: str | None = None,
    databricks_yml: Path | None = None,
) -> str:
    value = _resolve_variable(
        "pipeline_tag",
        "",
        override=override,
        target=target,
        databricks_yml=databricks_yml,
    )
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def pipeline_tag_var_ref() -> str:
    return PIPELINE_TAG_VAR_REF


def pipeline_max_update_retry_attempts_var_ref() -> str:
    return PIPELINE_MAX_UPDATE_RETRY_ATTEMPTS_VAR_REF


def pipeline_spark_version_var_ref() -> str:
    return PIPELINE_SPARK_VERSION_VAR_REF


def pipeline_cluster_num_workers_var_ref() -> str:
    return PIPELINE_CLUSTER_NUM_WORKERS_VAR_REF


def lakeflow_single_user_var_ref() -> str:
    return LAKEFLOW_SINGLE_USER_VAR_REF


def resolve_lakeflow_single_user(
    override: str | None = None,
    target: str | None = None,
    databricks_yml: Path | None = None,
) -> str:
    value = _resolve_variable(
        "lakeflow_single_user",
        "",
        override=override,
        target=target,
        databricks_yml=databricks_yml,
    )
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()
