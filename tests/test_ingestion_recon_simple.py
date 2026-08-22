"""Tests for simplified CT-driven recon helpers."""

from unittest.mock import MagicMock

from common.ops.ingestion_recon_ops import count_delta_table_rows, evaluate_simple_recon
from common.ops.lakeflow_event_ops import FlowSummaryRow
from common.ops.recon_store import resolve_uc_table_ref, UcTableRef, is_streaming_uc_table
from common.ops.sql_server_audit_store import CtPendingCounts
from datetime import datetime, timezone


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
