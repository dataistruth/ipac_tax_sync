/*
    ipac_metadata — control schema on SQL Server master database.
    Run in SSMS connected to master.
*/
USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'ipac_metadata')
BEGIN
    EXEC(N'CREATE SCHEMA ipac_metadata AUTHORIZATION dbo;');
END;
GO

PRINT 'Schema master.ipac_metadata is ready.';
GO
