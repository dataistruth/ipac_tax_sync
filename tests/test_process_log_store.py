"""Tests for shared process_log store."""

from datetime import datetime, timezone

import pytest

from common.ops.process_log_store import (
    ARTIFACT_TYPE_JOB,
    ARTIFACT_TYPE_NOTEBOOK,
    ARTIFACT_TYPE_PIPELINE,
    PROCESS_TYPE_CALC,
    PROCESS_TYPE_INGEST,
    PROCESS_TYPE_TRANSFER,
    build_process_log_row,
    client_nm_from_ingest_pipeline,
    ingest_log_rows_from_poll_snapshots,
    normalize_artifact_type,
    normalize_process_type,
    resolve_artifact,
    truncate_log,
)


def test_truncate_log_at_2000():
    assert len(truncate_log("x" * 3000)) == 2000
    assert truncate_log("short") == "short"


def test_normalize_process_type_accepts_calc_transfer():
    assert normalize_process_type("calc") == PROCESS_TYPE_CALC
    assert normalize_process_type("TRANSFER") == PROCESS_TYPE_TRANSFER


def test_normalize_process_type_rejects_unknown():
    with pytest.raises(ValueError, match="process_type"):
        normalize_process_type("unknown_workflow")


def test_client_nm_from_ingest_pipeline():
    assert client_nm_from_ingest_pipeline("p_iPC_2025_Dev7_15447_1") == "iPC_2025_Dev7_15447"


def test_build_process_log_row_calc():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 1, 0, 5, tzinfo=timezone.utc)
    row = build_process_log_row(
        PROCESS_TYPE_CALC,
        "sdt_allocation_run",
        "SUCCESS",
        artifact_type=ARTIFACT_TYPE_JOB,
        artifact_id="job-def-1",
        artifact_run_id="job-run-99",
        client_nm="iPC_2025_Dev7_15447",
        object_nm="AllocationResult",
        job_id="job-def-1",
        task_id="task-1",
        start_tm=start,
        end_tm=end,
        rows_read=1000,
        rows_written=950,
        log="calc completed",
    )
    d = row.as_dict()
    assert d["process_type"] == "calc"
    assert d["artifact_type"] == "job"
    assert d["artifact_id"] == "job-def-1"
    assert d["artifact_run_id"] == "job-run-99"
    assert d["process_id"] == "job-run-99"
    assert d["process_nm"] == "sdt_allocation_run"
    assert d["current_status"] == "SUCCESS"
    assert d["rows_read"] == 1000
    assert d["duration_sec"] == 300.0


def test_resolve_artifact_notebook():
    artifact_type, artifact_id = resolve_artifact(notebook_path="/Workspace/foo/calc.py")
    assert artifact_type == ARTIFACT_TYPE_NOTEBOOK
    assert artifact_id == "/Workspace/foo/calc.py"


def test_normalize_artifact_type_rejects_unknown():
    with pytest.raises(ValueError, match="artifact_type"):
        normalize_artifact_type("pipeline_task")


def test_build_process_log_row_transfer():
    row = build_process_log_row(
        PROCESS_TYPE_TRANSFER,
        "blob_to_volume_batch_42",
        "RUNNING",
        process_id="transfer-42",
        object_nm="client_a_raw/files",
        log="copy in progress",
    )
    assert row.process_type == "transfer"
    assert row.process_nm == "blob_to_volume_batch_42"


class _Snap:
    def __init__(self) -> None:
        self.logical_name = "p_client_a_1"
        self.pipeline_id = "pid-1"
        self.healthy = True
        self.reason = "healthy | heartbeat_age=10s"
        self.update_state = "RUNNING"
        self.continuous = True
        self.heartbeat_age_sec = 10
        self.heartbeat_threshold_sec = 900
        self.start_tm_ms = 1_700_000_000_000
        self.end_tm_ms = None
        self.poll_iteration = 1
        self.monitor_run_id = "run-1"
        self.update_id = "update-abc"


def test_ingest_log_rows_from_poll_snapshots():
    rows = ingest_log_rows_from_poll_snapshots([_Snap()])
    assert len(rows) == 1
    assert rows[0].process_type == PROCESS_TYPE_INGEST
    assert rows[0].artifact_type == ARTIFACT_TYPE_PIPELINE
    assert rows[0].artifact_id == "pid-1"
    assert rows[0].artifact_run_id == "update-abc"
    assert rows[0].process_id == "update-abc"
    assert rows[0].process_nm == "p_client_a_1"
    assert rows[0].detail_status == "RUNNING"
