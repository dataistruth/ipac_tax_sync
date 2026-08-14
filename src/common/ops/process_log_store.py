"""Unity Catalog Delta process_log — shared operational log for all iPAC processes."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

ProcessType = Literal["ingest", "calc", "transfer", "transform", "egress"]
ArtifactType = Literal["pipeline", "job", "notebook"]

PROCESS_TYPE_INGEST: ProcessType = "ingest"
PROCESS_TYPE_CALC: ProcessType = "calc"
PROCESS_TYPE_TRANSFER: ProcessType = "transfer"
PROCESS_TYPE_TRANSFORM: ProcessType = "transform"
PROCESS_TYPE_EGRESS: ProcessType = "egress"

ARTIFACT_TYPE_PIPELINE: ArtifactType = "pipeline"
ARTIFACT_TYPE_JOB: ArtifactType = "job"
ARTIFACT_TYPE_NOTEBOOK: ArtifactType = "notebook"

VALID_PROCESS_TYPES: frozenset[str] = frozenset(
    {PROCESS_TYPE_INGEST, PROCESS_TYPE_CALC, PROCESS_TYPE_TRANSFER, PROCESS_TYPE_TRANSFORM, PROCESS_TYPE_EGRESS}
)
VALID_ARTIFACT_TYPES: frozenset[str] = frozenset(
    {ARTIFACT_TYPE_PIPELINE, ARTIFACT_TYPE_JOB, ARTIFACT_TYPE_NOTEBOOK}
)

MAX_LOG_LEN = 2000
PROCESS_LOG_TABLE = "process_log"

_PIPELINE_CLIENT_RE = re.compile(r"^p_(.+)_\d+$", re.IGNORECASE)


@dataclass
class ProcessLogRow:
    """One row in ipac_metadata.process_log — any process type."""

    process_type: str
    process_nm: str
    current_status: str
    recorded_at: datetime
    artifact_type: str = ""
    artifact_id: str = ""
    artifact_run_id: str = ""
    process_id: str = ""
    client_nm: str = ""
    object_nm: str = ""
    job_id: str = ""
    task_id: str = ""
    start_tm: datetime | None = None
    end_tm: datetime | None = None
    detail_status: str = ""
    heartbeat_age_sec: int | None = None
    heartbeat_threshold_sec: int | None = None
    rows_read: int | None = None
    rows_written: int | None = None
    rows_deleted: int | None = None
    duration_sec: float | None = None
    poll_iteration: int | None = None
    monitor_run_id: str = ""
    log: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "log_id": str(uuid.uuid4()),
            "process_type": self.process_type,
            "process_nm": self.process_nm,
            "artifact_type": self.artifact_type,
            "artifact_id": self.artifact_id,
            "artifact_run_id": self.artifact_run_id,
            "process_id": self.process_id,
            "client_nm": self.client_nm,
            "object_nm": self.object_nm,
            "job_id": self.job_id,
            "task_id": self.task_id,
            "start_tm": self.start_tm,
            "end_tm": self.end_tm,
            "current_status": self.current_status,
            "detail_status": self.detail_status,
            "heartbeat_age_sec": self.heartbeat_age_sec,
            "heartbeat_threshold_sec": self.heartbeat_threshold_sec,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_deleted": self.rows_deleted,
            "duration_sec": self.duration_sec,
            "poll_iteration": self.poll_iteration,
            "monitor_run_id": self.monitor_run_id,
            "log": self.log,
            "recorded_at": self.recorded_at,
        }


def truncate_log(message: str | None, max_len: int = MAX_LOG_LEN) -> str:
    if not message:
        return ""
    text = str(message).strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def normalize_artifact_type(artifact_type: str) -> str:
    value = str(artifact_type).strip().lower()
    if value not in VALID_ARTIFACT_TYPES:
        raise ValueError(f"artifact_type must be one of {sorted(VALID_ARTIFACT_TYPES)}: {artifact_type}")
    return value


def resolve_artifact(
    artifact_type: str = "",
    artifact_id: str = "",
    process_id: str = "",
    job_id: str = "",
    notebook_path: str = "",
) -> tuple[str, str]:
    """Resolve artifact_type + artifact_id from explicit values or legacy id fields."""
    if artifact_type and artifact_id:
        return normalize_artifact_type(artifact_type), str(artifact_id).strip()
    if artifact_type and process_id:
        return normalize_artifact_type(artifact_type), str(process_id).strip()
    if artifact_type and job_id:
        return normalize_artifact_type(artifact_type), str(job_id).strip()
    if artifact_type and notebook_path:
        return normalize_artifact_type(artifact_type), str(notebook_path).strip()
    if process_id:
        return ARTIFACT_TYPE_PIPELINE, str(process_id).strip()
    if job_id:
        return ARTIFACT_TYPE_JOB, str(job_id).strip()
    if notebook_path:
        return ARTIFACT_TYPE_NOTEBOOK, str(notebook_path).strip()
    return "", ""


def normalize_process_type(process_type: str) -> str:
    value = str(process_type).strip().lower()
    if value not in VALID_PROCESS_TYPES:
        raise ValueError(f"process_type must be one of {sorted(VALID_PROCESS_TYPES)}: {process_type}")
    return value


def logical_process_name(display_name: str) -> str:
    """Strip bundle dev prefix from workspace display name."""
    text = str(display_name).strip()
    if text.startswith("[") and "] " in text:
        return text.split("] ", 1)[1].strip()
    return text


def client_nm_from_ingest_pipeline(process_nm: str) -> str:
    logical = logical_process_name(process_nm)
    match = _PIPELINE_CLIENT_RE.match(logical)
    if match:
        return match.group(1)
    return ""


def qualified_table(catalog: str, schema: str) -> str:
    return f"{catalog}.{schema}.{PROCESS_LOG_TABLE}"


def process_log_create_sql(catalog: str, schema: str) -> str:
    table = qualified_table(catalog, schema)
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  log_id STRING NOT NULL COMMENT 'Unique row id',
  process_type STRING NOT NULL COMMENT 'ingest | calc | transfer | transform | egress',
  process_nm STRING NOT NULL COMMENT 'Pipeline, job, transfer batch, or step name',
  artifact_type STRING COMMENT 'pipeline | job | notebook — stable Databricks artifact',
  artifact_id STRING COMMENT 'pipeline_id, job definition id, or notebook path',
  artifact_run_id STRING COMMENT 'Per-run id: pipeline update_id, job_run_id, task_run_id',
  process_id STRING COMMENT 'Legacy alias — same as artifact_run_id when set, else artifact_id',
  client_nm STRING COMMENT 'Client identifier when applicable',
  object_nm STRING COMMENT 'Table, calc module, file set, or dataset name',
  job_id STRING COMMENT 'Databricks job id',
  task_id STRING COMMENT 'Databricks task run id',
  start_tm TIMESTAMP COMMENT 'Process or step start time',
  end_tm TIMESTAMP COMMENT 'Process or step end time when known',
  current_status STRING NOT NULL COMMENT 'RUNNING | SUCCESS | FAILED | HEALTHY | UNHEALTHY | IDLE | SKIPPED',
  detail_status STRING COMMENT 'Sub-status: pipeline update_state, job life cycle, etc.',
  heartbeat_age_sec BIGINT COMMENT 'Seconds since last activity (ingest monitoring)',
  heartbeat_threshold_sec BIGINT COMMENT 'Stale threshold used for this poll',
  rows_read BIGINT COMMENT 'Rows read (calc/transfer/ingest metrics)',
  rows_written BIGINT COMMENT 'Rows written',
  rows_deleted BIGINT COMMENT 'Rows deleted',
  duration_sec DOUBLE COMMENT 'Elapsed seconds when end_tm is set',
  poll_iteration BIGINT COMMENT 'Monitor poll iteration for long-running watchers',
  monitor_run_id STRING COMMENT 'Watcher job run id when applicable',
  log STRING COMMENT 'Detail message (max 2000 chars)',
  recorded_at TIMESTAMP NOT NULL COMMENT 'When this row was written'
)
USING DELTA
COMMENT 'iPAC operational process_log — ingest, calc, transfer, and other workloads'
""".strip()


def ms_to_datetime(ms: int | None) -> datetime | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def build_process_log_row(
    process_type: str,
    process_nm: str,
    current_status: str,
    *,
    artifact_type: str = "",
    artifact_id: str = "",
    artifact_run_id: str = "",
    process_id: str = "",
    notebook_path: str = "",
    client_nm: str = "",
    object_nm: str = "",
    job_id: str = "",
    task_id: str = "",
    start_tm: datetime | None = None,
    end_tm: datetime | None = None,
    detail_status: str = "",
    heartbeat_age_sec: int | None = None,
    heartbeat_threshold_sec: int | None = None,
    rows_read: int | None = None,
    rows_written: int | None = None,
    rows_deleted: int | None = None,
    duration_sec: float | None = None,
    poll_iteration: int | None = None,
    monitor_run_id: str = "",
    log: str | None = None,
    recorded_at: datetime | None = None,
) -> ProcessLogRow:
    """Build one process_log row for any workload (ingest, calc, transfer, ...)."""
    now = recorded_at or datetime.now(timezone.utc)
    start = start_tm or now
    computed_duration: float | None = duration_sec
    if computed_duration is None and end_tm is not None:
        computed_duration = (end_tm - start).total_seconds()

    resolved_artifact_type, resolved_artifact_id = resolve_artifact(
        artifact_type=artifact_type,
        artifact_id=artifact_id,
        process_id=process_id,
        job_id=job_id,
        notebook_path=notebook_path,
    )
    resolved_run_id = str(artifact_run_id or process_id or "").strip()
    # process_id column kept for compatibility: prefer per-run id, else stable artifact id
    resolved_process_id = resolved_run_id or resolved_artifact_id

    return ProcessLogRow(
        process_type=normalize_process_type(process_type),
        process_nm=str(process_nm).strip(),
        current_status=str(current_status).strip().upper(),
        recorded_at=now,
        artifact_type=resolved_artifact_type,
        artifact_id=resolved_artifact_id,
        artifact_run_id=resolved_run_id,
        process_id=resolved_process_id,
        client_nm=client_nm,
        object_nm=object_nm,
        job_id=job_id,
        task_id=task_id,
        start_tm=start,
        end_tm=end_tm,
        detail_status=detail_status,
        heartbeat_age_sec=heartbeat_age_sec,
        heartbeat_threshold_sec=heartbeat_threshold_sec,
        rows_read=rows_read,
        rows_written=rows_written,
        rows_deleted=rows_deleted,
        duration_sec=computed_duration,
        poll_iteration=poll_iteration,
        monitor_run_id=monitor_run_id,
        log=truncate_log(log),
    )


def ensure_process_log_table(spark, catalog: str, schema: str) -> None:
    spark.sql(process_log_create_sql(catalog, schema))


def write_process_log_rows(
    spark,
    catalog: str,
    schema: str,
    rows: list[ProcessLogRow],
) -> int:
    if not rows:
        return 0
    ensure_process_log_table(spark, catalog, schema)
    df = spark.createDataFrame([row.as_dict() for row in rows])
    df.write.format("delta").mode("append").saveAsTable(qualified_table(catalog, schema))
    return len(rows)


def ingest_log_rows_from_poll_snapshots(
    snapshots: list[Any],
    recorded_at: datetime | None = None,
) -> list[ProcessLogRow]:
    """Map ingest pipeline poll snapshots to process_log rows (process_type=ingest)."""
    now = recorded_at or datetime.now(timezone.utc)
    rows: list[ProcessLogRow] = []
    for snap in snapshots:
        current_status = "HEALTHY" if snap.healthy else "UNHEALTHY"
        if snap.update_state in ("NONE", "") and not snap.continuous:
            current_status = "IDLE"
        if snap.update_state in ("FAILED", "CANCELED", "CANCELLED"):
            current_status = "FAILED"

        rows.append(
            build_process_log_row(
                PROCESS_TYPE_INGEST,
                snap.logical_name,
                current_status,
                artifact_type=ARTIFACT_TYPE_PIPELINE,
                artifact_id=snap.pipeline_id,
                artifact_run_id=snap.update_id or "",
                process_id=snap.update_id or snap.pipeline_id,
                client_nm=client_nm_from_ingest_pipeline(snap.logical_name),
                start_tm=ms_to_datetime(snap.start_tm_ms) or now,
                end_tm=ms_to_datetime(snap.end_tm_ms),
                detail_status=snap.update_state,
                heartbeat_age_sec=snap.heartbeat_age_sec,
                heartbeat_threshold_sec=snap.heartbeat_threshold_sec,
                poll_iteration=snap.poll_iteration,
                monitor_run_id=snap.monitor_run_id,
                log=snap.reason,
                recorded_at=now,
            )
        )
    return rows


# Backward-compatible aliases
IngestProcessLogRow = ProcessLogRow


def write_ingest_process_log_rows(
    spark,
    catalog: str,
    schema: str,
    rows: list[ProcessLogRow],
) -> int:
    return write_process_log_rows(spark, catalog, schema, rows)
