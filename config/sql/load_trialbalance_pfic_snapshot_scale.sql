-- =============================================================================
-- Scale test: bulk load dbo.TrialBalanceAdjustments_SnapShot and
--             dbo.PFICFootnoteInput_Snapshot
-- Edit @DatabaseName, @ClientID, @TaxPeriodID, and row targets, then run in SSMS.
-- PK_ID is IDENTITY — not included in INSERT.
-- =============================================================================
SET NOCOUNT ON;

DECLARE @DatabaseName              sysname = N'iPC_2025_DEV7_15447';  -- << change test DB
DECLARE @ClientID                  int     = 15447;
DECLARE @TaxPeriodID               int     = 2025;
DECLARE @TargetRowsTrialBalance    bigint  = 10000;   -- << rows for TrialBalanceAdjustments_SnapShot
DECLARE @TargetRowsPFIC            bigint  = 10000;   -- << rows for PFICFootnoteInput_Snapshot
DECLARE @BatchSize                 int     = 50000;

DECLARE @Inserted bigint;
DECLARE @ThisBatch int;
DECLARE @BatchStart bigint;
DECLARE @Start datetime2(3);
DECLARE @TableName nvarchar(512);
DECLARE @sql nvarchar(max);

IF DB_ID(@DatabaseName) IS NULL
BEGIN
    RAISERROR('Database not found: %s', 16, 1, @DatabaseName);
    RETURN;
END;

-- -----------------------------------------------------------------------------
-- dbo.TrialBalanceAdjustments_SnapShot
-- -----------------------------------------------------------------------------
IF @TargetRowsTrialBalance > 0
BEGIN
    SET @Inserted = 0;
    SET @Start = SYSUTCDATETIME();
    SET @TableName = QUOTENAME(@DatabaseName) + N'.[dbo].[TrialBalanceAdjustments_SnapShot]';

    IF OBJECT_ID(@TableName, N'U') IS NULL
    BEGIN
        RAISERROR('Table not found: %s', 16, 1, @TableName);
        RETURN;
    END;

    PRINT '=== TrialBalanceAdjustments_SnapShot ===';
    PRINT 'Target table : ' + @TableName;
    PRINT 'Target rows  : ' + CAST(@TargetRowsTrialBalance AS varchar(20));
    PRINT 'Started UTC  : ' + CONVERT(varchar(30), @Start, 126);

    WHILE @Inserted < @TargetRowsTrialBalance
    BEGIN
        SET @ThisBatch = CASE
            WHEN @TargetRowsTrialBalance - @Inserted > @BatchSize
                THEN @BatchSize
            ELSE CAST(@TargetRowsTrialBalance - @Inserted AS int)
        END;
        SET @BatchStart = @Inserted;

        SET @sql = N'
INSERT INTO ' + @TableName + N' (
    WorkflowID,
    TransactionID,
    AdjustmentID,
    EntityID,
    ClientID,
    TaxPeriodID,
    SourceTypeID,
    FieldID,
    CategoryID,
    BookReClass,
    M1,
    Eliminations,
    AJE1,
    AJE2,
    AJE3,
    RunID,
    PerBook,
    BookAdjustments,
    LineID,
    TBWorkflowID,
    IsEditToZero,
    AJC1,
    AJD1,
    AJC2,
    AJD2,
    AJC3,
    AJD3,
    AJC4,
    AJD4,
    Comments,
    TextValue
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
    N''tb_adj load batch '' + CAST(@BatchStart AS nvarchar(20)) + N'' row '' + CAST(rn AS nvarchar(20)),
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

        IF (@Inserted % (@BatchSize * 2) = 0) OR (@Inserted = @TargetRowsTrialBalance)
            PRINT CONCAT(
                'TrialBalance inserted ', @Inserted, ' / ', @TargetRowsTrialBalance,
                ' (', DATEDIFF(second, @Start, SYSUTCDATETIME()), ' sec)'
            );
    END;

    SET @sql = N'SELECT COUNT(*) AS row_count FROM ' + @TableName + N';';
    EXEC sys.sp_executesql @sql;
    PRINT CONCAT('TrialBalance done. Seconds: ', DATEDIFF(second, @Start, SYSUTCDATETIME()));
END;

-- -----------------------------------------------------------------------------
-- dbo.PFICFootnoteInput_Snapshot
-- -----------------------------------------------------------------------------
IF @TargetRowsPFIC > 0
BEGIN
    SET @Inserted = 0;
    SET @Start = SYSUTCDATETIME();
    SET @TableName = QUOTENAME(@DatabaseName) + N'.[dbo].[PFICFootnoteInput_Snapshot]';

    IF OBJECT_ID(@TableName, N'U') IS NULL
    BEGIN
        RAISERROR('Table not found: %s', 16, 1, @TableName);
        RETURN;
    END;

    PRINT '=== PFICFootnoteInput_Snapshot ===';
    PRINT 'Target table : ' + @TableName;
    PRINT 'Target rows  : ' + CAST(@TargetRowsPFIC AS varchar(20));
    PRINT 'Started UTC  : ' + CONVERT(varchar(30), @Start, 126);

    WHILE @Inserted < @TargetRowsPFIC
    BEGIN
        SET @ThisBatch = CASE
            WHEN @TargetRowsPFIC - @Inserted > @BatchSize
                THEN @BatchSize
            ELSE CAST(@TargetRowsPFIC - @Inserted AS int)
        END;
        SET @BatchStart = @Inserted;

        SET @sql = N'
INSERT INTO ' + @TableName + N' (
    WorkflowID,
    PFICFootnoteID,
    LineID,
    Amount,
    TextValue,
    ClientID,
    TaxPeriodID
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

        IF (@Inserted % (@BatchSize * 2) = 0) OR (@Inserted = @TargetRowsPFIC)
            PRINT CONCAT(
                'PFIC inserted ', @Inserted, ' / ', @TargetRowsPFIC,
                ' (', DATEDIFF(second, @Start, SYSUTCDATETIME()), ' sec)'
            );
    END;

    SET @sql = N'SELECT COUNT(*) AS row_count FROM ' + @TableName + N';';
    EXEC sys.sp_executesql @sql;
    PRINT CONCAT('PFIC done. Seconds: ', DATEDIFF(second, @Start, SYSUTCDATETIME()));
END;

-- -----------------------------------------------------------------------------
-- Single-row examples (PK_ID omitted — identity)
-- -----------------------------------------------------------------------------
/*
USE [iPC_2025_DEV7_15447];

INSERT INTO dbo.TrialBalanceAdjustments_SnapShot (
    WorkflowID, TransactionID, AdjustmentID, EntityID, ClientID, TaxPeriodID,
    SourceTypeID, FieldID, CategoryID,
    BookReClass, M1, Eliminations, AJE1, AJE2, AJE3,
    RunID, PerBook, BookAdjustments, LineID, TBWorkflowID,
    IsEditToZero, AJC1, AJD1, AJC2, AJD2, AJC3, AJD3, AJC4, AJD4,
    Comments, TextValue
)
VALUES (
    1, 1, 1, 1, 15447, 2025,
    1, 1, 1,
    100.50, 10.25, 5.00, 1.00, 2.00, 3.00,
    1, 500.00, 25.00, 1, 1,
    0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8,
    N'sample trial balance adjustment', N'sample_tb'
);

INSERT INTO dbo.PFICFootnoteInput_Snapshot (
    WorkflowID, PFICFootnoteID, LineID, Amount, TextValue, ClientID, TaxPeriodID
)
VALUES (
    1, 1, 1, 123.45, N'sample pfic footnote', 15447, 2025
);
*/
