/*
    CT reconciliation + pipeline poll metadata — ipac_metadata.dbo
    Prerequisite: 001_create_database.sql

    Single script for all recon/CT tables (CREATE IF NOT EXISTS pattern).
*/
USE ipac_metadata;
GO

IF OBJECT_ID(N'dbo.ct_db_watermark', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.ct_db_watermark (
        database_name   sysname       NOT NULL,
        client_nm       sysname       NULL,
        last_version    bigint        NOT NULL,
        checked_at      datetime2(3)  NOT NULL CONSTRAINT DF_ct_db_wm_checked DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_ct_db_watermark PRIMARY KEY CLUSTERED (database_name)
    );
END;
GO

IF OBJECT_ID(N'dbo.ct_table_watermark', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.ct_table_watermark (
        database_name   sysname       NOT NULL,
        schema_name     sysname       NOT NULL,
        table_name      sysname       NOT NULL,
        client_nm       sysname       NULL,
        pipeline_key    nvarchar(128) NULL,
        last_version    bigint        NOT NULL,
        updated_at      datetime2(3)  NOT NULL CONSTRAINT DF_ct_tbl_wm_updated DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_ct_table_watermark PRIMARY KEY CLUSTERED (database_name, schema_name, table_name)
    );
END;
GO

IF OBJECT_ID(N'dbo.recon_event_log_watermark', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.recon_event_log_watermark (
        pipeline_id           nvarchar(64)  NOT NULL,
        pipeline_key          nvarchar(128) NULL,
        last_event_ts         datetime2(3)  NULL,
        last_event_id         nvarchar(128) NULL,
        last_update_id        nvarchar(64)  NULL,
        last_api_update_state nvarchar(32)  NULL,
        last_poll_at          datetime2(3)  NULL,
        CONSTRAINT PK_recon_event_log_watermark PRIMARY KEY CLUSTERED (pipeline_id)
    );

    CREATE INDEX IX_recon_event_log_wm_poll
        ON dbo.recon_event_log_watermark (last_poll_at DESC);
END;
GO

IF OBJECT_ID(N'dbo.recon_run', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.recon_run (
        recon_run_id    uniqueidentifier NOT NULL CONSTRAINT DF_recon_run_id DEFAULT NEWSEQUENTIALID(),
        client_nm       sysname          NOT NULL,
        database_name   sysname          NOT NULL,
        pipeline_id     nvarchar(64)     NOT NULL,
        pipeline_key    nvarchar(128)    NULL,
        update_id       nvarchar(64)     NOT NULL,
        ct_head_version bigint           NULL,
        started_at      datetime2(3)     NOT NULL CONSTRAINT DF_recon_run_started DEFAULT SYSUTCDATETIME(),
        completed_at    datetime2(3)     NULL,
        run_status      varchar(20)      NOT NULL,
        run_message     nvarchar(4000)   NULL,
        CONSTRAINT PK_recon_run PRIMARY KEY CLUSTERED (recon_run_id)
    );

    CREATE INDEX IX_recon_run_client_update
        ON dbo.recon_run (client_nm, update_id, started_at DESC);
END;
GO

IF OBJECT_ID(N'dbo.recon_table_result', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.recon_table_result (
        result_id           bigint           NOT NULL IDENTITY(1,1),
        recon_run_id        uniqueidentifier NULL,
        client_nm           sysname          NOT NULL,
        database_name       sysname          NOT NULL,
        schema_name         sysname          NOT NULL,
        table_name          sysname          NOT NULL,
        pipeline_id         nvarchar(64)     NOT NULL,
        update_id           nvarchar(64)     NOT NULL,
        flow_name           nvarchar(256)    NULL,
        recon_type          int              NOT NULL,
        watermark_before    bigint           NOT NULL,
        ct_head_version     bigint           NOT NULL,
        pending_inserts     bigint           NOT NULL DEFAULT 0,
        pending_updates     bigint           NOT NULL DEFAULT 0,
        pending_deletes     bigint           NOT NULL DEFAULT 0,
        pending_total       bigint           NOT NULL DEFAULT 0,
        ingest_upserted     bigint           NOT NULL DEFAULT 0,
        ingest_deleted      bigint           NOT NULL DEFAULT 0,
        ingest_change_rows  bigint           NOT NULL DEFAULT 0,
        sync_status         varchar(20)      NOT NULL,
        recon_message       nvarchar(4000)   NULL,
        watermark_advanced  bit              NOT NULL DEFAULT 0,
        recorded_at         datetime2(3)     NOT NULL CONSTRAINT DF_recon_tbl_recorded DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_recon_table_result PRIMARY KEY CLUSTERED (result_id)
    );

    CREATE INDEX IX_recon_table_result_lookup
        ON dbo.recon_table_result (database_name, schema_name, table_name, recorded_at DESC);

    CREATE INDEX IX_recon_table_result_update
        ON dbo.recon_table_result (pipeline_id, update_id, table_name);
END;
GO

IF OBJECT_ID(N'dbo.ingestion_audit_log', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.ingestion_audit_log (
        audit_id        bigint           NOT NULL IDENTITY(1,1),
        event_type      varchar(40)      NOT NULL,
        client_nm       sysname          NULL,
        database_name   sysname          NULL,
        object_name     sysname          NULL,
        pipeline_id     nvarchar(64)     NULL,
        update_id       nvarchar(64)     NULL,
        detail_json     nvarchar(max)    NULL,
        recorded_at     datetime2(3)     NOT NULL CONSTRAINT DF_ing_audit_recorded DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_ingestion_audit_log PRIMARY KEY CLUSTERED (audit_id)
    );

    CREATE INDEX IX_ingestion_audit_log_type_time
        ON dbo.ingestion_audit_log (event_type, recorded_at DESC);
END;
GO

/* Optional monitoring views */
CREATE OR ALTER VIEW dbo.v_latest_db_watermarks
AS
SELECT database_name, client_nm, last_version, checked_at
FROM dbo.ct_db_watermark;
GO

CREATE OR ALTER VIEW dbo.v_latest_table_watermarks
AS
SELECT database_name, schema_name, table_name, client_nm, pipeline_key, last_version, updated_at
FROM dbo.ct_table_watermark;
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
    result_id, recon_run_id, client_nm, database_name, schema_name, table_name,
    pipeline_id, update_id, flow_name, recon_type, watermark_before, ct_head_version,
    pending_inserts, pending_updates, pending_deletes, pending_total,
    ingest_upserted, ingest_deleted, ingest_change_rows,
    sync_status, recon_message, watermark_advanced, recorded_at
FROM ranked
WHERE rn = 1;
GO

PRINT 'CT recon tables + views ready in ipac_metadata.dbo.';
GO
