/*
    ipac_metadata — dedicated database for CT watermarks and recon audit.
    Run in SSMS (any database context). Do not store app tables in master.

    Prerequisite: none
*/
IF DB_ID(N'ipac_metadata') IS NULL
BEGIN
    CREATE DATABASE ipac_metadata;
END;
GO

USE ipac_metadata;
GO

PRINT 'Database ipac_metadata is ready (default schema dbo).';
GO
