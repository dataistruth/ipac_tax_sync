"""Tests for process_log-driven restart and alert helpers."""

from datetime import datetime, timezone

from common.ops.alert_ops import format_pipeline_restart_alert
from common.ops.process_log_store import (
    ProcessLogPipelineStatus,
    latest_ingest_pipeline_status_sql,
    process_log_indicates_failed_restart,
)


def test_process_log_indicates_failed_restart():
    failed = ProcessLogPipelineStatus(
        process_nm="p_client_1",
        current_status="FAILED",
        detail_status="FAILED",
        log="UNHEALTHY",
        recorded_at=None,
        artifact_id="pid",
        artifact_run_id="upd",
        client_nm="client",
    )
    unhealthy = ProcessLogPipelineStatus(
        process_nm="p_client_1",
        current_status="UNHEALTHY",
        detail_status="RUNNING",
        log="stale",
        recorded_at=None,
        artifact_id="pid",
        artifact_run_id="upd",
        client_nm="client",
    )
    assert process_log_indicates_failed_restart(failed)
    assert not process_log_indicates_failed_restart(unhealthy)


def test_latest_ingest_pipeline_status_sql_escapes_quotes():
    sql = latest_ingest_pipeline_status_sql("dev7", "ipac_metadata", ["p_test_1", "p_o'brien_1"])
    assert "p_o''brien_1" in sql
    assert "process_type = 'ingest'" in sql


def test_format_pipeline_restart_alert():
    failed_at = datetime(2026, 8, 16, 13, 52, 51, tzinfo=timezone.utc)
    subject, body = format_pipeline_restart_alert(
        "p_client_1",
        "pipeline-id-1",
        failed_at,
        "latest_update.state=FAILED",
    )
    assert "p_client_1" in subject
    assert "FAILED" in body
    assert "Restart requested" in body
