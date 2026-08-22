/*
    DEPRECATED — do not run.

    process_log lives only in Unity Catalog Delta ({catalog}.ipac_metadata.process_log).
    Heartbeat monitor and restart-failed-pipelines read/write UC only.

    Use: common.ops.process_log_store + monitor_pipeline_heartbeat.py
*/
GO
