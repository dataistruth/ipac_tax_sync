/*
    Grants for ipac audit SQL login.
    Prerequisite: 003_recon_audit_tables.sql

    Replace @AuditLogin with your Databricks audit SQL user before running.
*/
USE master;
GO

DECLARE @AuditLogin sysname = N'YOUR_AUDIT_SQL_LOGIN';  -- e.g. ipac_audit_user

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @AuditLogin)
BEGIN
    RAISERROR('Login/user %s not found in master. Create the login first.', 16, 1, @AuditLogin);
    RETURN;
END;

GRANT SELECT, INSERT, UPDATE ON SCHEMA::ipac_metadata TO [YOUR_AUDIT_SQL_LOGIN];
GO

/*
    Repeat per client database (CT-enabled) — example for 15447:
*/
USE [iPC_2025_DEV7_15447];
GO

IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'YOUR_AUDIT_SQL_LOGIN')
BEGIN
    GRANT VIEW CHANGE TRACKING TO [YOUR_AUDIT_SQL_LOGIN];
    GRANT SELECT ON SCHEMA::dbo TO [YOUR_AUDIT_SQL_LOGIN];
END;
GO

PRINT 'Review and apply GRANT VIEW CHANGE TRACKING on each active client database.';
GO
