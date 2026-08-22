/*
    Grants for ipac_metadata.dbo + Change Tracking read on client DBs.

    Prerequisite: 002_ct_recon_tables.sql

    Use your EXISTING admin SQL login — no new login required.
    Edit @AuditLogin below (same value in both sections).
*/
USE ipac_metadata;
GO

DECLARE @AuditLogin sysname = N'YOUR_ADMIN_SQL_LOGIN';  -- e.g. your Databricks SQL admin user

IF @AuditLogin IN (N'YOUR_ADMIN_SQL_LOGIN', N'')
BEGIN
    RAISERROR('Set @AuditLogin to your existing admin SQL user (line 14), then re-run.', 16, 1);
    RETURN;
END;

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @AuditLogin)
BEGIN
    DECLARE @createUserSql nvarchar(max) =
        N'CREATE USER ' + QUOTENAME(@AuditLogin) + N' FOR LOGIN ' + QUOTENAME(@AuditLogin) + N';';
    EXEC sys.sp_executesql @createUserSql;
    PRINT 'Created user in ipac_metadata for login ' + @AuditLogin;
END;

IF IS_MEMBER('db_owner') = 1 OR EXISTS (
    SELECT 1
    FROM sys.database_role_members drm
    JOIN sys.database_principals r ON r.principal_id = drm.role_principal_id
    JOIN sys.database_principals m ON m.principal_id = drm.member_principal_id
    WHERE r.name = N'db_owner' AND m.name = @AuditLogin
)
BEGIN
    PRINT 'User already db_owner in ipac_metadata — dbo grants optional.';
END
ELSE
BEGIN
    DECLARE @sql nvarchar(max) =
        N'GRANT SELECT, INSERT, UPDATE ON SCHEMA::dbo TO ' + QUOTENAME(@AuditLogin) + N';';
    EXEC sys.sp_executesql @sql;
    PRINT 'Granted dbo on ipac_metadata to ' + @AuditLogin;
END;
GO

/*
    Client database — example iPC_2025_DEV7_15447.
    Repeat for each CT-enabled client DB (15347, 15350, etc.).
    VIEW CHANGE TRACKING must be granted ON SCHEMA or ON each CT table (not bare TO user).
*/
USE [iPC_2025_DEV7_15447];
GO

DECLARE @AuditLogin sysname = N'YOUR_ADMIN_SQL_LOGIN';  -- same login as above

IF @AuditLogin IN (N'YOUR_ADMIN_SQL_LOGIN', N'')
BEGIN
    RAISERROR('Set @AuditLogin to your existing admin SQL user (line 55), then re-run.', 16, 1);
    RETURN;
END;

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @AuditLogin)
BEGIN
    RAISERROR('User [%s] not mapped in this database.', 16, 1, @AuditLogin);
    RETURN;
END;

IF IS_MEMBER('db_owner') = 1
   OR EXISTS (
        SELECT 1
        FROM sys.database_role_members drm
        JOIN sys.database_principals r ON r.principal_id = drm.role_principal_id
        JOIN sys.database_principals m ON m.principal_id = drm.member_principal_id
        WHERE r.name = N'db_owner' AND m.name = @AuditLogin
   )
BEGIN
    PRINT 'User already db_owner in iPC_2025_DEV7_15447 — CT grants optional.';
END
ELSE
BEGIN
    DECLARE @grantSql nvarchar(max) = N'';

    SELECT @grantSql = @grantSql +
        N'GRANT VIEW CHANGE TRACKING ON '
        + QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name)
        + N' TO ' + QUOTENAME(@AuditLogin) + N';'
    FROM sys.change_tracking_tables AS ct
    INNER JOIN sys.tables AS t ON t.object_id = ct.object_id;

    SET @grantSql = @grantSql +
        N'GRANT SELECT ON SCHEMA::dbo TO ' + QUOTENAME(@AuditLogin) + N';';

    EXEC sys.sp_executesql @grantSql;
    PRINT 'Granted VIEW CHANGE TRACKING on CT tables + SELECT on dbo to ' + @AuditLogin;
END;
GO
