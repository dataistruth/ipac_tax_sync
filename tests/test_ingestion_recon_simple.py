"""Tests for simplified CT-driven recon helpers."""

from unittest.mock import MagicMock

from common.ops.ingestion_recon_ops import (
    count_delta_table_rows,
    evaluate_ct_delta_history_recon,
    evaluate_ingest_quiesce_recon,
    evaluate_simple_recon,
    evaluate_table_refresh_after_sql_ct,
    select_row_count_sample_tables,
    summarize_delta_history_refresh,
)
from common.ops.lakeflow_event_ops import FlowSummaryRow
from common.ops.recon_store import FlowMetricsRow, resolve_uc_table_ref, UcTableRef, is_streaming_uc_table
from common.ops.sql_server_audit_store import CtPendingCounts, PendingCtTable
from datetime import datetime, timedelta, timezone


def _summary(upserted: int = 1, deleted: int = 0) -> FlowSummaryRow:
    now = datetime.now(timezone.utc)
    return FlowSummaryRow(
        pipeline_id="pid",
        pipeline_name="p_test",
        update_id="upd",
        flow_name="dbo.Table_snapshot_flow",
        table_name="Table",
        client_nm="client",
        destination_schema="client_poc1",
        destination_table="Table",
        recon_type=1,
        final_flow_status="COMPLETED",
        total_output_rows=upserted,
        total_upserted=upserted,
        total_deleted=deleted,
        total_change_rows=upserted + deleted,
        total_output_bytes=0,
        first_event_time=now,
        last_event_time=now,
    )


def test_select_row_count_sample_tables():
    probes = [
        PendingCtTable(
            schema_name="dbo",
            table_name="Small",
            watermark_before=1,
            ct_head_version=10,
            pending=CtPendingCounts(inserts=5),
        ),
        PendingCtTable(
            schema_name="dbo",
            table_name="Big",
            watermark_before=1,
            ct_head_version=10,
            pending=CtPendingCounts(inserts=50000),
        ),
        PendingCtTable(
            schema_name="dbo",
            table_name="Mid",
            watermark_before=1,
            ct_head_version=10,
            pending=CtPendingCounts(inserts=100),
        ),
    ]
    sample = select_row_count_sample_tables(probes, sample_size=2)
    assert sample == {"big", "mid"}


def test_evaluate_ct_delta_history_pass_history_only():
    merge_at = datetime(2026, 8, 22, 1, 0, 0, tzinfo=timezone.utc)
    sql_ct = datetime(2026, 8, 21, 22, 59, 28, tzinfo=timezone.utc)
    refresh = {
        "last_refreshed_at": merge_at,
        "latest_refresh_status": "MERGE",
        "delta_version": 20,
        "source": "delta_history",
    }
    status, msg = evaluate_ct_delta_history_recon(
        refresh,
        sql_ct,
        quiesce_sec=15,
        pending=CtPendingCounts(inserts=50000),
        recon_type=1,
        sql_row_count=None,
        delta_row_count=None,
        require_row_count=False,
    )
    assert status == "PASS"
    assert "ct_delta_history" in msg


def test_evaluate_ct_delta_history_fail_sample_mismatch():
    merge_at = datetime(2026, 8, 22, 1, 0, 0, tzinfo=timezone.utc)
    sql_ct = datetime(2026, 8, 21, 22, 59, 28, tzinfo=timezone.utc)
    refresh = {"last_refreshed_at": merge_at, "latest_refresh_status": "MERGE"}
    status, msg = evaluate_ct_delta_history_recon(
        refresh,
        sql_ct,
        15,
        CtPendingCounts(inserts=10),
        1,
        100,
        99,
        require_row_count=True,
    )
    assert status == "FAIL"
    assert "sample mismatch" in msg


def test_summarize_delta_history_refresh_prefers_merge():
    rows = [
        {
            "version": 22,
            "timestamp": "2026-08-22T01:47:01.000+00:00",
            "operation": "DLT SETUP",
            "operationParameters": '{"updateId":"407beb35-a183-4040-adb5-4ef98a818198"}',
        },
        {
            "version": 21,
            "timestamp": "2026-08-22T01:19:49.000+00:00",
            "operation": "DLT SETUP",
            "operationParameters": '{"updateId":"baac7ad6-42e2-4ac8-89f3-a2ec18b5729d"}',
        },
        {
            "version": 20,
            "timestamp": "2026-08-22T00:48:29.000+00:00",
            "operation": "MERGE",
            "operationParameters": "{}",
        },
    ]
    info = summarize_delta_history_refresh(rows)
    assert info is not None
    assert info["source"] == "delta_history"
    assert info["delta_version"] == 20
    assert info["latest_refresh_status"] == "MERGE"
    assert info["last_merge_version"] == 20
    assert info["last_dlt_setup_version"] == 22
    assert info["dlt_update_id"] == "407beb35-a183-4040-adb5-4ef98a818198"


def test_evaluate_table_refresh_after_sql_ct_pass_with_delta_history():
    merge_at = datetime(2026, 8, 22, 0, 48, 29, tzinfo=timezone.utc)
    sql_ct = datetime(2026, 8, 21, 22, 59, 28, tzinfo=timezone.utc)
    refresh = {
        "table": "dev7.schema.K1Input_Snapshot",
        "last_refreshed_at": merge_at,
        "latest_refresh_status": "MERGE",
        "source": "delta_history",
        "delta_version": 20,
    }
    status, msg = evaluate_table_refresh_after_sql_ct(
        refresh,
        sql_ct,
        quiesce_sec=15,
    )
    assert status == "PASS"
    assert "delta_version=20" in msg


def test_evaluate_table_refresh_after_sql_ct_pass():
    now = datetime.now(timezone.utc)
    refresh = {
        "table": "dev7.schema.Table",
        "last_refreshed_at": now,
        "latest_refresh_status": "SUCCESS",
    }
    status, msg = evaluate_table_refresh_after_sql_ct(
        refresh,
        now - timedelta(seconds=60),
        quiesce_sec=15,
    )
    assert status == "PASS"
    assert "delta write after SQL CT" in msg


def test_evaluate_ingest_quiesce_pass():
    now = datetime.now(timezone.utc)
    metrics = [
        FlowMetricsRow(
            event_id="e1",
            pipeline_id="pid",
            pipeline_name="p",
            update_id="upd",
            flow_name="dbo.Table_snapshot_flow",
            table_name="Table",
            event_timestamp=now,
            flow_status="RUNNING",
            output_rows=100,
            rows_upserted=100,
            rows_deleted=0,
            output_bytes=0,
            client_nm="c",
            destination_schema="s",
            destination_table="Table",
        )
    ]
    table_refresh = {
        "table": "dev7.schema.K1Input_Snapshot",
        "last_refreshed_at": now,
        "latest_refresh_status": "SUCCESS",
        "last_refresh_type": "FULL",
        "source": "refresh_information",
    }
    status, msg = evaluate_ingest_quiesce_recon(
        metrics,
        CtPendingCounts(inserts=100),
        recon_type=1,
        table_refresh,
        now - timedelta(seconds=60),
        quiesce_sec=15,
    )
    assert status == "PASS"
    assert "ingest_quiesce" in msg


def test_evaluate_ingest_quiesce_waiting_no_metrics():
    now = datetime.now(timezone.utc)
    status, msg = evaluate_ingest_quiesce_recon(
        [],
        CtPendingCounts(inserts=5),
        recon_type=1,
        {"last_refreshed_at": now, "table": "t"},
        now - timedelta(seconds=60),
    )
    assert status == "WAITING"
    assert "flow_progress" in msg


def test_evaluate_simple_flow_complete():
    status, msg = evaluate_simple_recon(
        _summary(),
        CtPendingCounts(inserts=5),
        recon_type=1,
        sql_row_count=100,
        delta_row_count=99,
        pass_rule="auto",
    )
    assert status == "PASS"
    assert "COMPLETED" in msg


def test_evaluate_simple_row_count_match():
    status, msg = evaluate_simple_recon(
        None,
        CtPendingCounts(inserts=1),
        recon_type=1,
        sql_row_count=100,
        delta_row_count=100,
        pass_rule="row_count",
    )
    assert status == "PASS"
    assert "row_count match" in msg


def test_evaluate_simple_waiting():
    status, msg = evaluate_simple_recon(
        None,
        CtPendingCounts(inserts=3),
        recon_type=1,
        None,
        None,
        pass_rule="flow_complete",
    )
    assert status == "WAITING"
    assert "CT pending" in msg


def test_evaluate_simple_api_update_complete():
    status, msg = evaluate_simple_recon(
        None,
        CtPendingCounts(inserts=5),
        recon_type=1,
        None,
        None,
        pass_rule="flow_complete",
        api_update_complete=True,
    )
    assert status == "PASS"
    assert "API last update COMPLETED" in msg


def test_evaluate_simple_waiting_row_count_deferred():
    status, msg = evaluate_simple_recon(
        None,
        CtPendingCounts(inserts=3),
        recon_type=1,
        None,
        None,
        pass_rule="row_count",
    )
    assert status == "WAITING"
    assert "before COUNT_BIG" in msg


def test_evaluate_simple_waiting_row_count_unavailable():
    status, msg = evaluate_simple_recon(
        None,
        CtPendingCounts(inserts=3),
        recon_type=1,
        sql_row_count=100,
        delta_row_count=None,
        pass_rule="row_count",
    )
    assert status == "WAITING"
    assert "row_count unavailable" in msg


def test_resolve_uc_table_ref_case_insensitive():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False
    spark.sql.side_effect = [
        MagicMock(collect=MagicMock(return_value=[("ipc_2025_dev7_15447_poc_1")])),
        MagicMock(
            collect=MagicMock(
                return_value=[MagicMock(tableName="K1Input_Snapshot")]
            )
        ),
    ]
    ref = resolve_uc_table_ref(
        spark,
        "dev7",
        "iPC_2025_DEV7_15447_poc_1",
        "K1Input_Snapshot",
    )
    assert ref == UcTableRef(
        catalog="dev7",
        schema="ipc_2025_dev7_15447_poc_1",
        table="K1Input_Snapshot",
    )


def test_count_delta_table_rows_uses_describe_detail():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True

    info_schema = MagicMock()
    info_schema.collect.return_value = [("MANAGED",)]

    detail = MagicMock()
    detail.select.return_value.first.return_value = {"numRecords": 42000}

    def sql_side_effect(stmt: str):
        if "information_schema" in stmt:
            return info_schema
        if "DESCRIBE DETAIL" in stmt:
            return detail
        return MagicMock()

    spark.sql.side_effect = sql_side_effect
    assert count_delta_table_rows(spark, "cat", "schema", "Table") == 42000
    assert any("DESCRIBE DETAIL" in str(c[0][0]) for c in spark.sql.call_args_list)


def test_count_delta_table_rows_streaming_uses_count_big():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True

    info_schema = MagicMock()
    info_schema.collect.return_value = [("STREAMING_TABLE",)]
    describe = MagicMock()
    describe.collect.return_value = []

    count_result = MagicMock()
    count_result.collect.return_value = [{"cnt": 8241287}]

    spark.sql.side_effect = [info_schema, count_result]

    assert count_delta_table_rows(spark, "dev7", "ipc_schema", "K1Input_Snapshot") == 8241287
    assert "COUNT_BIG" in spark.sql.call_args_list[-1][0][0]
