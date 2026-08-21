/*
    Operational process_log — ipac_metadata.dbo
    Mirrors Unity Catalog process_log shape for SQL-side ops (optional vs Delta).

    Prerequisite: 001_create_database.sql
*/
USE ipac_metadata;
GO

IF OBJECT_ID(N'dbo.process_log', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.process_log (
        log_id                  nvarchar(64)   NOT NULL,
        process_type            varchar(20)    NOT NULL,
        process_nm              nvarchar(256)  NOT NULL,
        artifact_type           varchar(20)    NULL,
        artifact_id             nvarchar(256)  NULL,
        artifact_run_id         nvarchar(128)  NULL,
        process_id              nvarchar(128)  NULL,
        client_nm               sysname        NULL,
        object_nm               sysname        NULL,
        job_id                  nvarchar(64)   NULL,
        task_id                 nvarchar(64)   NULL,
        start_tm                datetime2(3)   NULL,
        end_tm                  datetime2(3)   NULL,
        current_status          varchar(20)    NOT NULL,
        detail_status           varchar(40)    NULL,
        heartbeat_age_sec       bigint         NULL,
        heartbeat_threshold_sec bigint         NULL,
        rows_read               bigint         NULL,
        rows_written            bigint         NULL,
        rows_deleted            bigint         NULL,
        duration_sec            float          NULL,
        poll_iteration          bigint         NULL,
        monitor_run_id          nvarchar(64)   NULL,
        log                     nvarchar(2000) NULL,
        recorded_at             datetime2(3)   NOT NULL CONSTRAINT DF_process_log_recorded DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_process_log PRIMARY KEY CLUSTERED (log_id)
    );

    CREATE INDEX IX_process_log_process_time
        ON dbo.process_log (process_type, process_nm, recorded_at DESC);

    CREATE INDEX IX_process_log_client_time
        ON dbo.process_log (client_nm, recorded_at DESC);

    CREATE INDEX IX_process_log_artifact
        ON dbo.process_log (artifact_type, artifact_id, artifact_run_id);
END;
GO

PRINT 'Table ipac_metadata.dbo.process_log is ready.';
GO
