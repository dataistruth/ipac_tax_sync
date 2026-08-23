"""Project path helpers."""

from __future__ import annotations

from pathlib import Path


def project_root() -> Path:
    here = Path(__file__).resolve().parent
    for parent in [here, *here.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return here.parents[2]


def config_dir() -> Path:
    return project_root() / "config"


def common_config_dir() -> Path:
    return config_dir() / "common"


def client_overrides_dir() -> Path:
    return common_config_dir() / "client_overrides"


def src_dir() -> Path:
    return project_root() / "src"


def util_dir() -> Path:
    return src_dir() / "util"


def src_common_dir() -> Path:
    return src_dir() / "common"


def client_src_dir(client_nm: str) -> Path:
    return src_dir() / client_nm


def client_pipelines_dir(client_nm: str) -> Path:
    return client_src_dir(client_nm) / "pipelines"


def client_transform_dir(client_nm: str) -> Path:
    return client_src_dir(client_nm) / "transform"


def client_sql_dir(client_nm: str) -> Path:
    return client_src_dir(client_nm) / "sql"


def generated_bundle_dir() -> Path:
    return project_root() / "generated" / "bundle"


def generated_schema_dir() -> Path:
    return project_root() / "generated" / "schema"


def generated_config_dir() -> Path:
    return project_root() / "generated" / "config"


def generated_config_schema_dir() -> Path:
    """Bundle UC schema resources generated per client (deployed before pipelines)."""
    return generated_config_dir() / "schema"


def resources_jobs_dir() -> Path:
    return project_root() / "resources" / "jobs"


def resources_instance_pools_dir() -> Path:
    return project_root() / "resources" / "instance_pools"
