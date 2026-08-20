"""Tests for Lakeflow ingestion flow reconciliation."""

from datetime import datetime, timezone

from common.ops.ingestion_recon_ops import pipeline_needs_event_log_poll
from common.ops.lakeflow_event_ops import (
    aggregate_flow_metrics,
    build_pipeline_recon_context,
    evaluate_recon,
    flow_progress_extract_sql,
    parse_flow_progress_event,
    resolve_table_from_flow_name,
    TableReconConfig,
)
from common.ops.recon_store import ReconEventLogWatermark
from common.ops.source_ct_ops import (
    build_ct_count_sql,
    build_federated_ct_count_sql,
)


def _ts(minute: int) -> datetime:
    return datetime(2026, 8, 15, 12, minute, 0, tzinfo=timezone.utc)


def _table_cfg() -> list[TableReconConfig]:
    return [
        TableReconConfig(
            table_nm="CustomImportDetail",
            recon_type=2,
            destination_schema="clientpoc_1",
            destination_table="CustomImportDetail",
        ),
    ]


def test_flow_progress_extract_sql_uses_hidden_event_log():
    sql = flow_progress_extract_sql("pid-abc", lookback_hours=6)
    assert "event_log('pid-abc')" in sql
    assert "MANAGED_INGESTION" in sql
    assert "flow_progress" in sql
    assert "INTERVAL 6 HOURS" in sql


def test_flow_progress_extract_sql_since_timestamp():
    since = datetime(2026, 8, 15, 10, 0, 0, tzinfo=timezone.utc)
    sql = flow_progress_extract_sql("pid-abc", since_timestamp=since)
    assert "timestamp > timestamp '2026-08-15 10:00:00'" in sql
    assert "INTERVAL" not in sql


def test_pipeline_needs_event_log_poll_initial():
    needs, reason = pipeline_needs_event_log_poll({"state": "RUNNING"}, None)
    assert needs
    assert "initial" in reason


def test_pipeline_needs_event_log_poll_skips_steady_terminal():
    wm = ReconEventLogWatermark(
        pipeline_id="pid-1",
        last_update_id="upd-1",
        last_api_update_state="COMPLETED",
        last_poll_at=_ts(1),
    )
    detail = {
        "state": "IDLE",
        "latest_update": {"update_id": "upd-1", "state": "COMPLETED"},
    }
    needs, reason = pipeline_needs_event_log_poll(detail, wm)
    assert not needs
    assert "steady" in reason


def test_pipeline_needs_event_log_poll_new_update_id():
    wm = ReconEventLogWatermark(
        pipeline_id="pid-1",
        last_update_id="upd-1",
        last_api_update_state="COMPLETED",
    )
    detail = {"latest_update": {"update_id": "upd-2", "state": "RUNNING"}}
    needs, reason = pipeline_needs_event_log_poll(detail, wm)
    assert needs
    assert "new update_id" in reason


def test_parse_and_aggregate_completed_flow():
    ctx = build_pipeline_recon_context("p_iPC_2025_Dev7_15347_1", _table_cfg(), pipeline_id="pid-1")
    rows = [
        {
            "event_id": "e1",
            "pipeline_id": "pid-1",
            "pipeline_name": "p_iPC_2025_Dev7_15347_1",
            "update_id": "upd-1",
            "flow_name": "dbo_CustomImportDetail_flow",
            "table_name": "CustomImportDetail",
            "event_timestamp": _ts(0),
            "flow_status": "RUNNING",
            "output_rows": 10,
            "rows_upserted": 8,
            "rows_deleted": 2,
            "output_bytes": 100,
        },
        {
            "event_id": "e2",
            "pipeline_id": "pid-1",
            "pipeline_name": "p_iPC_2025_Dev7_15347_1",
            "update_id": "upd-1",
            "flow_name": "dbo_CustomImportDetail_flow",
            "table_name": "CustomImportDetail",
            "event_timestamp": _ts(5),
            "flow_status": "COMPLETED",
            "output_rows": 5,
            "rows_upserted": 4,
            "rows_deleted": 1,
            "output_bytes": 50,
        },
    ]
    metrics = [parse_flow_progress_event(r, ctx) for r in rows]
    metrics = [m for m in metrics if m]
    assert len(metrics) == 2

    summaries = aggregate_flow_metrics(metrics, _table_cfg())
    assert len(summaries) == 1
    s = summaries[0]
    assert s.final_flow_status == "COMPLETED"
    assert s.total_upserted == 12
    assert s.total_deleted == 3
    assert s.total_change_rows == 15
    assert s.recon_type == 2


def test_aggregate_skips_non_completed_flow():
    ctx = build_pipeline_recon_context("p_test_1", _table_cfg())
    row = parse_flow_progress_event(
        {
            "event_id": "e1",
            "update_id": "u1",
            "flow_name": "f1",
            "event_timestamp": _ts(0),
            "flow_status": "RUNNING",
            "rows_upserted": 1,
            "rows_deleted": 0,
        },
        ctx,
    )
    assert row is not None
    assert aggregate_flow_metrics([row], _table_cfg()) == []


def test_evaluate_recon_type_1_pass_without_source():
    summary = aggregate_flow_metrics(
        [
            parse_flow_progress_event(
                {
                    "event_id": "e1",
                    "update_id": "u1",
                    "flow_name": "dbo_CustomImportDetail_flow",
                    "table_name": "CustomImportDetail",
                    "event_timestamp": _ts(1),
                    "flow_status": "COMPLETED",
                    "rows_upserted": 5,
                    "rows_deleted": 1,
                },
                build_pipeline_recon_context("p_test_1", [
                    TableReconConfig("CustomImportDetail", 1, "s", "CustomImportDetail"),
                ]),
            ),
        ],
        [TableReconConfig("CustomImportDetail", 1, "s", "CustomImportDetail")],
    )[0]
    result = evaluate_recon(summary, None)
    assert result.recon_status == "PASS"


def test_evaluate_recon_type_2_compare_change_rows():
    summary = aggregate_flow_metrics(
        [
            parse_flow_progress_event(
                {
                    "event_id": "e1",
                    "update_id": "u1",
                    "flow_name": "f",
                    "table_name": "CustomImportDetail",
                    "event_timestamp": _ts(1),
                    "flow_status": "COMPLETED",
                    "rows_upserted": 10,
                    "rows_deleted": 2,
                },
                build_pipeline_recon_context("p_test_1", _table_cfg()),
            ),
        ],
        _table_cfg(),
    )[0]
    assert evaluate_recon(summary, 12).recon_status == "PASS"
    assert evaluate_recon(summary, 11).recon_status == "FAIL"


def test_resolve_table_from_flow_name():
    cfgs = _table_cfg()
    assert resolve_table_from_flow_name("dbo_CustomImportDetail_flow", cfgs) == "CustomImportDetail"
    assert (
        resolve_table_from_flow_name(
            "dev7.iPC_2025_Dev7_15347_poc1.CustomImportDetail_cdc_flow",
            cfgs,
        )
        == "CustomImportDetail"
    )
    assert (
        resolve_table_from_flow_name("dev7.iPC_2025_Dev7_15347_poc1.CustomImportDetail", cfgs)
        == "CustomImportDetail"
    )


def test_flow_progress_extract_sql_parses_table_from_flow_name():
    sql = flow_progress_extract_sql("pid-abc")
    assert "origin.dataset_name" in sql
    assert "regexp_replace(origin.flow_name" in sql


def test_ct_count_sql_uses_changetable_operations():
    start = _ts(0)
    end = _ts(10)
    sql = build_ct_count_sql("dbo", "Entity", start, end, recon_type=2)
    assert "CHANGETABLE(CHANGES dbo.Entity, 0)" in sql
    assert "SYS_CHANGE_OPERATION IN ('I', 'U', 'D')" in sql
    assert "sys.dm_tran_commit_time" in sql

    sql3 = build_ct_count_sql("dbo", "Entity", start, end, recon_type=3)
    assert "SYS_CHANGE_OPERATION IN ('I', 'U')" in sql3

    fed = build_federated_ct_count_sql("src_cat", "dbo", "Entity", start, end, 2)
    assert "CHANGETABLE(CHANGES src_cat.dbo.Entity, 0)" in fed
