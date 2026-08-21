/*
    Optional one-time migration: master.ipac_metadata schema -> ipac_metadata.dbo

    Run only if you previously deployed tables under master.ipac_metadata.
    Safe to re-run (skips when source tables are missing or target already has rows).

    Prerequisite: 001–003 on ipac_metadata database
*/
USE ipac_metadata;
GO

IF OBJECT_ID(N'master.ipac_metadata.ct_db_watermark', N'U') IS NULL
   AND OBJECT_ID(N'master.ipac_metadata.ct_table_watermark', N'U') IS NULL
BEGIN
    PRINT 'No master.ipac_metadata tables — migration not required.';
    RETURN;
END;

DECLARE @sql nvarchar(max);

IF OBJECT_ID(N'master.ipac_metadata.ct_db_watermark', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM dbo.ct_db_watermark)
BEGIN
  SET @sql = N'
    INSERT INTO dbo.ct_db_watermark (database_name, client_nm, last_version, checked_at)
    SELECT database_name, client_nm, last_version, checked_at
    FROM master.ipac_metadata.ct_db_watermark;';
    EXEC sys.sp_executesql @sql;
    PRINT 'Migrated ct_db_watermark from master.ipac_metadata.';
END;

IF OBJECT_ID(N'master.ipac_metadata.ct_table_watermark', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM dbo.ct_table_watermark)
BEGIN
    SET @sql = N'
    INSERT INTO dbo.ct_table_watermark (
        database_name, schema_name, table_name, client_nm, pipeline_key, last_version, updated_at
    )
    SELECT database_name, schema_name, table_name, client_nm, pipeline_key, last_version, updated_at
    FROM master.ipac_metadata.ct_table_watermark;';
    EXEC sys.sp_executesql @sql;
    PRINT 'Migrated ct_table_watermark from master.ipac_metadata.';
END;

IF OBJECT_ID(N'master.ipac_metadata.recon_run', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM dbo.recon_run)
BEGIN
    SET @sql = N'
    INSERT INTO dbo.recon_run (
        recon_run_id, client_nm, database_name, pipeline_id, pipeline_key,
        update_id, ct_head_version, started_at, completed_at, run_status, run_message
    )
    SELECT
        recon_run_id, client_nm, database_name, pipeline_id, pipeline_key,
        update_id, ct_head_version, started_at, completed_at, run_status, run_message
    FROM master.ipac_metadata.recon_run;';
    EXEC sys.sp_executesql @sql;
    PRINT 'Migrated recon_run from master.ipac_metadata.';
END;

IF OBJECT_ID(N'master.ipac_metadata.recon_table_result', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM dbo.recon_table_result)
BEGIN
    SET @sql = N'
    INSERT INTO dbo.recon_table_result (
        recon_run_id, client_nm, database_name, schema_name, table_name,
        pipeline_id, update_id, flow_name, recon_type, watermark_before, ct_head_version,
        pending_inserts, pending_updates, pending_deletes, pending_total,
        ingest_upserted, ingest_deleted, ingest_change_rows,
        sync_status, recon_message, watermark_advanced, recorded_at
    )
    SELECT
        recon_run_id, client_nm, database_name, schema_name, table_name,
        pipeline_id, update_id, flow_name, recon_type, watermark_before, ct_head_version,
        pending_inserts, pending_updates, pending_deletes, pending_total,
        ingest_upserted, ingest_deleted, ingest_change_rows,
        sync_status, recon_message, watermark_advanced, recorded_at
    FROM master.ipac_metadata.recon_table_result;';
    EXEC sys.sp_executesql @sql;
    PRINT 'Migrated recon_table_result from master.ipac_metadata.';
END;

IF OBJECT_ID(N'master.ipac_metadata.ingestion_audit_log', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM dbo.ingestion_audit_log)
BEGIN
    SET @sql = N'
    INSERT INTO dbo.ingestion_audit_log (
        event_type, client_nm, database_name, object_name, pipeline_id, update_id, detail_json, recorded_at
    )
    SELECT event_type, client_nm, database_name, object_name, pipeline_id, update_id, detail_json, recorded_at
    FROM master.ipac_metadata.ingestion_audit_log;';
    EXEC sys.sp_executesql @sql;
    PRINT 'Migrated ingestion_audit_log from master.ipac_metadata.';
END;

PRINT 'Migration from master.ipac_metadata complete (or nothing to migrate).';
GO
