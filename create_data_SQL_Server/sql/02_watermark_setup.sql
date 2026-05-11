-- =============================================================================
-- File: 02_watermark_setup.sql
-- Project: MIGRATION-DBX-001
-- Description: Stored procedure called by ADF after each successful copy
-- Run on: migration-db (Azure SQL PaaS)
-- =============================================================================

USE [migration-db];
GO

-- ---------------------------------------------------------------------------
-- usp_update_watermark
-- Called by ADF Stored Procedure activity after successful copy
-- Updates last_watermark, last_run_ts, last_row_count per table
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.usp_update_watermark', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_update_watermark;
GO

CREATE PROCEDURE dbo.usp_update_watermark
    @table_name     VARCHAR(100),
    @new_watermark  DATETIME2,
    @row_count      BIGINT = 0
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.watermark_control
    SET
        last_watermark  = @new_watermark,
        last_run_ts     = SYSDATETIME(),
        last_row_count  = @row_count
    WHERE
        table_name = @table_name;

    -- Return updated row for ADF to log
    SELECT
        table_name,
        last_watermark,
        last_run_ts,
        last_row_count
    FROM dbo.watermark_control
    WHERE table_name = @table_name;
END;
GO

-- ---------------------------------------------------------------------------
-- usp_get_new_watermark
-- Called by ADF Lookup activity to get MAX(watermark_col) from source table
-- Returns the ceiling of what we should extract in this run
-- Dynamic SQL needed because table + column are parameterized
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.usp_get_new_watermark', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_get_new_watermark;
GO

CREATE PROCEDURE dbo.usp_get_new_watermark
    @table_name     VARCHAR(100),
    @watermark_col  VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql        NVARCHAR(500);
    DECLARE @new_wm     DATETIME2;

    SET @sql = N'SELECT @wm = MAX([' + @watermark_col + N']) FROM dbo.[' + @table_name + N']';

    EXEC sp_executesql
        @sql,
        N'@wm DATETIME2 OUTPUT',
        @wm = @new_wm OUTPUT;

    -- If table is empty or all NULLs, return current time as safety
    SELECT ISNULL(@new_wm, SYSDATETIME()) AS new_watermark;
END;
GO

-- ---------------------------------------------------------------------------
-- Verification queries (run after setup to confirm)
-- ---------------------------------------------------------------------------

-- Check watermark table state
SELECT
    table_name,
    watermark_col,
    CAST(last_watermark AS VARCHAR(30))  AS last_watermark,
    last_run_ts,
    last_row_count
FROM dbo.watermark_control
ORDER BY table_name;
GO

-- Test usp_get_new_watermark (will return current time since tables empty)
EXEC dbo.usp_get_new_watermark
    @table_name    = 'olist_orders_dataset',
    @watermark_col = 'order_purchase_timestamp';
GO

-- Test usp_update_watermark
EXEC dbo.usp_update_watermark
    @table_name    = 'olist_orders_dataset',
    @new_watermark = '2018-01-01 00:00:00',
    @row_count     = 999;

-- Reset it back
UPDATE dbo.watermark_control
SET last_watermark = '1900-01-01', last_run_ts = NULL, last_row_count = NULL
WHERE table_name = 'olist_orders_dataset';
GO
