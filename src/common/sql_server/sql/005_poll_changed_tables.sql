/*
    Poll all CT-enabled tables in one database and list tables with changes
    since the stored watermark in ipac_metadata.dbo.

    Edit @TargetDatabase before running.
*/
USE ipac_metadata;
GO

DECLARE @TargetDatabase sysname = N'iPC_2025_DEV7_15447';

DECLARE @since bigint;
DECLARE @until bigint;

SELECT @since = last_version
FROM dbo.ct_db_watermark
WHERE database_name = @TargetDatabase;

IF @since IS NULL
BEGIN
    RAISERROR('No DB watermark for [%s]. Insert a baseline row in ipac_metadata.dbo.ct_db_watermark first.', 16, 1, @TargetDatabase);
    RETURN;
END;

DECLARE @sql nvarchar(max) = N'
USE ' + QUOTENAME(@TargetDatabase) + N';
SET @until = CHANGE_TRACKING_CURRENT_VERSION();

IF OBJECT_ID(''tempdb..#changed_tables'') IS NOT NULL DROP TABLE #changed_tables;
CREATE TABLE #changed_tables (
    schema_name sysname NOT NULL,
    table_name  sysname NOT NULL,
    op          nchar(1) NOT NULL,
    change_count bigint NOT NULL
);

DECLARE @schema sysname, @table sysname, @q nvarchar(max);

DECLARE c CURSOR LOCAL FAST_FORWARD FOR
SELECT SCHEMA_NAME(t.schema_id), t.name
FROM sys.change_tracking_tables ctt
JOIN sys.tables t ON t.object_id = ctt.object_id
ORDER BY 1, 2;

OPEN c;
FETCH NEXT FROM c INTO @schema, @table;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @q = N''
    INSERT INTO #changed_tables (schema_name, table_name, op, change_count)
    SELECT @schema, @table, ct.SYS_CHANGE_OPERATION, COUNT_BIG(*)
    FROM CHANGETABLE(CHANGES '' + QUOTENAME(@schema) + N''.'' + QUOTENAME(@table) + N'', @since) ct
    WHERE ct.SYS_CHANGE_VERSION <= @until
    GROUP BY ct.SYS_CHANGE_OPERATION
    HAVING COUNT_BIG(*) > 0;'';
    EXEC sp_executesql @q, N''@schema sysname, @table sysname, @since bigint, @until bigint'',
        @schema, @table, @since, @until;
    FETCH NEXT FROM c INTO @schema, @table;
END;
CLOSE c; DEALLOCATE c;

SELECT @since AS since_version, @until AS until_version, @until - @since AS db_version_delta;

SELECT
    schema_name,
    table_name,
    SUM(CASE WHEN op = ''I'' THEN change_count ELSE 0 END) AS inserts,
    SUM(CASE WHEN op = ''U'' THEN change_count ELSE 0 END) AS updates,
    SUM(CASE WHEN op = ''D'' THEN change_count ELSE 0 END) AS deletes,
    SUM(change_count) AS total_changes
FROM #changed_tables
GROUP BY schema_name, table_name
ORDER BY total_changes DESC, schema_name, table_name;
';

EXEC sys.sp_executesql @sql, N'@since bigint, @until bigint OUTPUT', @since, @until OUTPUT;
GO
