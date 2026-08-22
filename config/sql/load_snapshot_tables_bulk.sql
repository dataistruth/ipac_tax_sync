-- =============================================================================
-- Bulk load: K1Input_Snapshot, TrialBalanceAdjustments_SnapShot,
--            PFICFootnoteInput_Snapshot
--
-- Edit the CONFIG block below, then run the entire script in SSMS.
-- PK_ID (identity) is omitted on tables that define it.
-- =============================================================================
SET NOCOUNT ON;

-- =========================== CONFIG — edit these ===========================
DECLARE @DatabaseName  sysname = N'iPC_2025_DEV7_15447';
DECLARE @TargetRows    bigint  = 10000;     -- rows per table (0 = skip that table section)
DECLARE @BatchSize     int     = 50000;
DECLARE @ClientID      int     = 15447;     -- match client for @DatabaseName
DECLARE @TaxPeriodID   int     = 2025;
-- ===========================================================================

DECLARE @Inserted      bigint;
DECLARE @ThisBatch     int;
DECLARE @BatchStart    bigint;
DECLARE @Start         datetime2(3);
DECLARE @ScriptStart   datetime2(3) = SYSUTCDATETIME();
DECLARE @TableName     nvarchar(512);
DECLARE @sql           nvarchar(max);
DECLARE @Target        bigint;

IF @BatchSize <= 0
BEGIN
    RAISERROR('@BatchSize must be > 0', 16, 1);
    RETURN;
END;

IF DB_ID(@DatabaseName) IS NULL
BEGIN
    RAISERROR('Database not found: %s', 16, 1, @DatabaseName);
    RETURN;
END;

PRINT 'Database     : ' + @DatabaseName;
PRINT 'Target rows    : ' + CAST(@TargetRows AS varchar(20)) + ' per table';
PRINT 'Batch size     : ' + CAST(@BatchSize AS varchar(20));
PRINT 'ClientID       : ' + CAST(@ClientID AS varchar(20));
PRINT 'TaxPeriodID    : ' + CAST(@TaxPeriodID AS varchar(20));
PRINT 'Script start   : ' + CONVERT(varchar(30), @ScriptStart, 126);
PRINT '';

-- =============================================================================
-- 1) dbo.K1Input_Snapshot
-- =============================================================================
SET @Target = @TargetRows;

IF @Target > 0
BEGIN
    SET @Inserted = 0;
    SET @Start = SYSUTCDATETIME();
    SET @TableName = QUOTENAME(@DatabaseName) + N'.[dbo].[K1Input_Snapshot]';

    IF OBJECT_ID(@TableName, N'U') IS NULL
        PRINT 'SKIP — table not found: ' + @TableName;
    ELSE
    BEGIN
        PRINT '=== K1Input_Snapshot ===';
        PRINT 'Table : ' + @TableName;

        WHILE @Inserted < @Target
        BEGIN
            SET @ThisBatch = CASE
                WHEN @Target - @Inserted > @BatchSize THEN @BatchSize
                ELSE CAST(@Target - @Inserted AS int)
            END;
            SET @BatchStart = @Inserted;

            SET @sql = N'
INSERT INTO ' + @TableName + N' (
    WorkflowID, K1PackageID, LineID, Amount, Adjustment,
    TextValue, TotalAmount, ClientID, TaxPeriodID
)
SELECT
    ((@BatchStart + rn) % 1000) + 1,
    ((@BatchStart + rn) % 500) + 1,
    ((@BatchStart + rn) % 10000) + 1,
    CAST((@BatchStart + rn) % 100000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 1000 AS float) / 10.0,
    LEFT(N''k1_'' + CAST(@BatchStart + rn AS varchar(20)), 60),
    CAST((@BatchStart + rn) % 200000 AS float) / 100.0,
    @ClientID,
    @TaxPeriodID
FROM (
    SELECT TOP (@ThisBatch)
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.all_objects AS a
    CROSS JOIN sys.all_objects AS b
) AS tally;';

            EXEC sys.sp_executesql
                @sql,
                N'@ThisBatch int, @BatchStart bigint, @ClientID int, @TaxPeriodID int',
                @ThisBatch = @ThisBatch,
                @BatchStart = @BatchStart,
                @ClientID = @ClientID,
                @TaxPeriodID = @TaxPeriodID;

            SET @Inserted = @Inserted + @ThisBatch;

            IF (@Inserted % (@BatchSize * 2) = 0) OR (@Inserted = @Target)
                PRINT CONCAT('  inserted ', @Inserted, '/', @Target,
                    ' (', DATEDIFF(second, @Start, SYSUTCDATETIME()), 's)');
        END;

        PRINT CONCAT('  done in ', DATEDIFF(second, @Start, SYSUTCDATETIME()), 's');
    END;
    PRINT '';
END;

-- =============================================================================
-- 2) dbo.TrialBalanceAdjustments_SnapShot  (note: capital S in SnapShot)
-- =============================================================================
SET @Target = @TargetRows;

IF @Target > 0
BEGIN
    SET @Inserted = 0;
    SET @Start = SYSUTCDATETIME();
    SET @TableName = QUOTENAME(@DatabaseName) + N'.[dbo].[TrialBalanceAdjustments_SnapShot]';

    IF OBJECT_ID(@TableName, N'U') IS NULL
        PRINT 'SKIP — table not found: ' + @TableName;
    ELSE
    BEGIN
        PRINT '=== TrialBalanceAdjustments_SnapShot ===';
        PRINT 'Table : ' + @TableName;

        WHILE @Inserted < @Target
        BEGIN
            SET @ThisBatch = CASE
                WHEN @Target - @Inserted > @BatchSize THEN @BatchSize
                ELSE CAST(@Target - @Inserted AS int)
            END;
            SET @BatchStart = @Inserted;

            SET @sql = N'
INSERT INTO ' + @TableName + N' (
    WorkflowID, TransactionID, AdjustmentID, EntityID, ClientID, TaxPeriodID,
    SourceTypeID, FieldID, CategoryID,
    BookReClass, M1, Eliminations, AJE1, AJE2, AJE3,
    RunID, PerBook, BookAdjustments, LineID, TBWorkflowID,
    IsEditToZero, AJC1, AJD1, AJC2, AJD2, AJC3, AJD3, AJC4, AJD4,
    Comments, TextValue
)
SELECT
    ((@BatchStart + rn) % 1000) + 1,
    ((@BatchStart + rn) % 5000) + 1,
    ((@BatchStart + rn) % 2000) + 1,
    ((@BatchStart + rn) % 500) + 1,
    @ClientID,
    @TaxPeriodID,
    ((@BatchStart + rn) % 20) + 1,
    ((@BatchStart + rn) % 50) + 1,
    ((@BatchStart + rn) % 30) + 1,
    CAST((@BatchStart + rn) % 100000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 50000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 25000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 10000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 8000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 6000 AS float) / 100.0,
    ((@BatchStart + rn) % 100) + 1,
    CAST((@BatchStart + rn) % 200000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 150000 AS float) / 100.0,
    ((@BatchStart + rn) % 10000) + 1,
    ((@BatchStart + rn) % 1000) + 1,
    CASE WHEN (@BatchStart + rn) % 2 = 0 THEN 1 ELSE 0 END,
    CAST((@BatchStart + rn) % 9000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 7000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 5000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 4000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 3000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 2000 AS float) / 100.0,
    CAST((@BatchStart + rn) % 1500 AS float) / 100.0,
    CAST((@BatchStart + rn) % 1000 AS float) / 100.0,
    N''tb load '' + CAST(@BatchStart AS nvarchar(20)) + N'' '' + CAST(rn AS nvarchar(20)),
    LEFT(N''tb_'' + CAST(@BatchStart + rn AS nvarchar(20)), 60)
FROM (
    SELECT TOP (@ThisBatch)
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.all_objects AS a
    CROSS JOIN sys.all_objects AS b
) AS tally;';

            EXEC sys.sp_executesql
                @sql,
                N'@ThisBatch int, @BatchStart bigint, @ClientID int, @TaxPeriodID int',
                @ThisBatch = @ThisBatch,
                @BatchStart = @BatchStart,
                @ClientID = @ClientID,
                @TaxPeriodID = @TaxPeriodID;

            SET @Inserted = @Inserted + @ThisBatch;

            IF (@Inserted % (@BatchSize * 2) = 0) OR (@Inserted = @Target)
                PRINT CONCAT('  inserted ', @Inserted, '/', @Target,
                    ' (', DATEDIFF(second, @Start, SYSUTCDATETIME()), 's)');
        END;

        PRINT CONCAT('  done in ', DATEDIFF(second, @Start, SYSUTCDATETIME()), 's');
    END;
    PRINT '';
END;

-- =============================================================================
-- 3) dbo.PFICFootnoteInput_Snapshot
-- =============================================================================
SET @Target = @TargetRows;

IF @Target > 0
BEGIN
    SET @Inserted = 0;
    SET @Start = SYSUTCDATETIME();
    SET @TableName = QUOTENAME(@DatabaseName) + N'.[dbo].[PFICFootnoteInput_Snapshot]';

    IF OBJECT_ID(@TableName, N'U') IS NULL
        PRINT 'SKIP — table not found: ' + @TableName;
    ELSE
    BEGIN
        PRINT '=== PFICFootnoteInput_Snapshot ===';
        PRINT 'Table : ' + @TableName;

        WHILE @Inserted < @Target
        BEGIN
            SET @ThisBatch = CASE
                WHEN @Target - @Inserted > @BatchSize THEN @BatchSize
                ELSE CAST(@Target - @Inserted AS int)
            END;
            SET @BatchStart = @Inserted;

            SET @sql = N'
INSERT INTO ' + @TableName + N' (
    WorkflowID, PFICFootnoteID, LineID, Amount, TextValue, ClientID, TaxPeriodID
)
SELECT
    ((@BatchStart + rn) % 1000) + 1,
    ((@BatchStart + rn) % 500) + 1,
    ((@BatchStart + rn) % 10000) + 1,
    CAST((@BatchStart + rn) % 100000 AS float) / 100.0,
    LEFT(N''pfic_'' + CAST(@BatchStart + rn AS nvarchar(20)), 100),
    @ClientID,
    @TaxPeriodID
FROM (
    SELECT TOP (@ThisBatch)
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM sys.all_objects AS a
    CROSS JOIN sys.all_objects AS b
) AS tally;';

            EXEC sys.sp_executesql
                @sql,
                N'@ThisBatch int, @BatchStart bigint, @ClientID int, @TaxPeriodID int',
                @ThisBatch = @ThisBatch,
                @BatchStart = @BatchStart,
                @ClientID = @ClientID,
                @TaxPeriodID = @TaxPeriodID;

            SET @Inserted = @Inserted + @ThisBatch;

            IF (@Inserted % (@BatchSize * 2) = 0) OR (@Inserted = @Target)
                PRINT CONCAT('  inserted ', @Inserted, '/', @Target,
                    ' (', DATEDIFF(second, @Start, SYSUTCDATETIME()), 's)');
        END;

        PRINT CONCAT('  done in ', DATEDIFF(second, @Start, SYSUTCDATETIME()), 's');
    END;
    PRINT '';
END;

-- =============================================================================
-- Summary row counts
-- =============================================================================
PRINT '=== Row counts ===';

IF OBJECT_ID(QUOTENAME(@DatabaseName) + N'.[dbo].[K1Input_Snapshot]', N'U') IS NOT NULL
BEGIN
    SET @sql = N'SELECT N''K1Input_Snapshot'' AS table_name, COUNT(*) AS row_count FROM '
        + QUOTENAME(@DatabaseName) + N'.[dbo].[K1Input_Snapshot];';
    EXEC sys.sp_executesql @sql;
END
ELSE
    PRINT 'K1Input_Snapshot — not found';

IF OBJECT_ID(QUOTENAME(@DatabaseName) + N'.[dbo].[TrialBalanceAdjustments_SnapShot]', N'U') IS NOT NULL
BEGIN
    SET @sql = N'SELECT N''TrialBalanceAdjustments_SnapShot'' AS table_name, COUNT(*) AS row_count FROM '
        + QUOTENAME(@DatabaseName) + N'.[dbo].[TrialBalanceAdjustments_SnapShot];';
    EXEC sys.sp_executesql @sql;
END
ELSE
    PRINT 'TrialBalanceAdjustments_SnapShot — not found';

IF OBJECT_ID(QUOTENAME(@DatabaseName) + N'.[dbo].[PFICFootnoteInput_Snapshot]', N'U') IS NOT NULL
BEGIN
    SET @sql = N'SELECT N''PFICFootnoteInput_Snapshot'' AS table_name, COUNT(*) AS row_count FROM '
        + QUOTENAME(@DatabaseName) + N'.[dbo].[PFICFootnoteInput_Snapshot];';
    EXEC sys.sp_executesql @sql;
END
ELSE
    PRINT 'PFICFootnoteInput_Snapshot — not found';

PRINT CONCAT('Script finished. Total seconds: ', DATEDIFF(second, @ScriptStart, SYSUTCDATETIME()));