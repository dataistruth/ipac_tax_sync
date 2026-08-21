/*
    Baseline CT watermarks — all CT-enabled databases on the instance.

    Prerequisite: 002_ct_recon_tables.sql
    Safe to re-run (MERGE).
*/
USE ipac_metadata;
GO

IF OBJECT_ID('tempdb..#db_ct_head') IS NOT NULL DROP TABLE #db_ct_head;
CREATE TABLE #db_ct_head (
    database_name   sysname NOT NULL PRIMARY KEY,
    ct_head_version bigint  NULL
);

DECLARE @db sysname;
DECLARE @q  nvarchar(max);

DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
SELECT d.name
FROM master.sys.change_tracking_databases AS ctd
INNER JOIN master.sys.databases AS d ON d.database_id = ctd.database_id
WHERE d.state_desc = 'ONLINE'
  AND d.user_access_desc <> 'SINGLE_USER'
ORDER BY d.name;

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @db;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @q = N'
        USE ' + QUOTENAME(@db) + N';
        INSERT INTO #db_ct_head (database_name, ct_head_version)
        VALUES (@db, CHANGE_TRACKING_CURRENT_VERSION());';

    EXEC sys.sp_executesql @q, N'@db sysname', @db;
    FETCH NEXT FROM db_cursor INTO @db;
END;

CLOSE db_cursor;
DEALLOCATE db_cursor;

MERGE dbo.ct_db_watermark AS target
USING (
    SELECT database_name, ct_head_version AS last_version, database_name AS client_nm
    FROM #db_ct_head
    WHERE ct_head_version IS NOT NULL
) AS source
ON target.database_name = source.database_name
WHEN MATCHED THEN
    UPDATE SET last_version = source.last_version, client_nm = source.client_nm, checked_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (database_name, client_nm, last_version, checked_at)
    VALUES (source.database_name, source.client_nm, source.last_version, SYSUTCDATETIME());

PRINT 'ct_db_watermark baseline complete.';

IF OBJECT_ID('tempdb..#tbl_ct_head') IS NOT NULL DROP TABLE #tbl_ct_head;
CREATE TABLE #tbl_ct_head (
    database_name sysname NOT NULL,
    schema_name   sysname NOT NULL,
    table_name    sysname NOT NULL,
    last_version  bigint  NOT NULL,
    PRIMARY KEY (database_name, schema_name, table_name)
);

DECLARE @db2 sysname;
DECLARE @sql2 nvarchar(max);

DECLARE db2_cursor CURSOR LOCAL FAST_FORWARD FOR
SELECT d.name
FROM master.sys.change_tracking_databases AS ctd
INNER JOIN master.sys.databases AS d ON d.database_id = ctd.database_id
WHERE d.state_desc = 'ONLINE'
  AND d.user_access_desc <> 'SINGLE_USER'
ORDER BY d.name;

OPEN db2_cursor;
FETCH NEXT FROM db2_cursor INTO @db2;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql2 = N'
        USE ' + QUOTENAME(@db2) + N';
        DECLARE @head bigint = CHANGE_TRACKING_CURRENT_VERSION();
        INSERT INTO #tbl_ct_head (database_name, schema_name, table_name, last_version)
        SELECT DB_NAME(), SCHEMA_NAME(t.schema_id), t.name, @head
        FROM sys.change_tracking_tables AS ct
        INNER JOIN sys.tables AS t ON t.object_id = ct.object_id;';

    EXEC sys.sp_executesql @sql2;
    FETCH NEXT FROM db2_cursor INTO @db2;
END;

CLOSE db2_cursor;
DEALLOCATE db2_cursor;

MERGE dbo.ct_table_watermark AS target
USING (
    SELECT database_name, schema_name, table_name, last_version, database_name AS client_nm
    FROM #tbl_ct_head
) AS source
ON target.database_name = source.database_name
 AND target.schema_name = source.schema_name
 AND target.table_name = source.table_name
WHEN MATCHED THEN
    UPDATE SET last_version = source.last_version, client_nm = source.client_nm, updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (database_name, schema_name, table_name, client_nm, pipeline_key, last_version, updated_at)
    VALUES (source.database_name, source.schema_name, source.table_name, source.client_nm, NULL, source.last_version, SYSUTCDATETIME());

PRINT 'ct_table_watermark baseline complete.';
GO
