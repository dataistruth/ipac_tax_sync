/*
    Views for SSMS monitoring of recon state.
    Prerequisite: 003_recon_audit_tables.sql
*/
USE ipac_metadata;
GO

CREATE OR ALTER VIEW dbo.v_latest_db_watermarks
AS
SELECT
    w.database_name,
    w.client_nm,
    w.last_version,
    w.checked_at
FROM dbo.ct_db_watermark AS w;
GO

CREATE OR ALTER VIEW dbo.v_latest_table_watermarks
AS
SELECT
    w.database_name,
    w.schema_name,
    w.table_name,
    w.client_nm,
    w.pipeline_key,
    w.last_version,
    w.updated_at
FROM dbo.ct_table_watermark AS w;
GO

CREATE OR ALTER VIEW dbo.v_latest_recon_table_result
AS
WITH ranked AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY r.database_name, r.schema_name, r.table_name
            ORDER BY r.recorded_at DESC
        ) AS rn
    FROM dbo.recon_table_result AS r
)
SELECT
    result_id,
    recon_run_id,
    client_nm,
    database_name,
    schema_name,
    table_name,
    pipeline_id,
    update_id,
    flow_name,
    recon_type,
    watermark_before,
    ct_head_version,
    pending_inserts,
    pending_updates,
    pending_deletes,
    pending_total,
    ingest_upserted,
    ingest_deleted,
    ingest_change_rows,
    sync_status,
    recon_message,
    watermark_advanced,
    recorded_at
FROM ranked
WHERE rn = 1;
GO

PRINT 'Views ipac_metadata.dbo.v_latest_* created.';
GO
