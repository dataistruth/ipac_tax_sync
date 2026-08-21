/*
    CT watermark tables — master.ipac_metadata
    Prerequisite: 001_create_schema.sql
*/
USE master;
GO

IF OBJECT_ID(N'ipac_metadata.ct_db_watermark', N'U') IS NULL
BEGIN
    CREATE TABLE ipac_metadata.ct_db_watermark (
        database_name   sysname       NOT NULL,
        client_nm       sysname       NULL,
        last_version    bigint        NOT NULL,
        checked_at      datetime2(3)  NOT NULL CONSTRAINT DF_ct_db_wm_checked DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_ct_db_watermark PRIMARY KEY CLUSTERED (database_name)
    );
END;
GO

IF OBJECT_ID(N'ipac_metadata.ct_table_watermark', N'U') IS NULL
BEGIN
    CREATE TABLE ipac_metadata.ct_table_watermark (
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

PRINT 'Watermark tables created.';
GO
