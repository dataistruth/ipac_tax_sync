/*
    Create ipac_metadata database (SQL Server metadata store).
    Run once on the instance. Tables: 002_ct_recon_tables.sql, 003_process_log_table.sql
*/
IF DB_ID(N'ipac_metadata') IS NULL
BEGIN
    CREATE DATABASE ipac_metadata;
END;
GO

PRINT 'Database ipac_metadata is ready.';
GO
