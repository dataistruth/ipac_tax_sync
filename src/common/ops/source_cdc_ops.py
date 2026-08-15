"""Backward-compatible re-exports — implementation is in source_ct_ops (SQL Server CT)."""

from common.ops.source_ct_ops import (
    build_ct_count_sql,
    build_federated_ct_count_sql,
    ingest_metric_for_recon_type,
    run_source_cdc_count,
    run_source_ct_count,
)

# Legacy names used in early recon drafts
build_cdc_count_sql = build_ct_count_sql
build_federated_cdc_count_sql = build_federated_ct_count_sql

__all__ = [
    "build_ct_count_sql",
    "build_cdc_count_sql",
    "build_federated_ct_count_sql",
    "build_federated_cdc_count_sql",
    "ingest_metric_for_recon_type",
    "run_source_ct_count",
    "run_source_cdc_count",
]
