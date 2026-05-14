-- Revert watermark_control key back to logical table name
UPDATE dbo.watermark_control
SET table_name = 'olist_order_payments_dataset'
WHERE table_name = 'vw_order_payments_incremental';

-- Verify
SELECT *
FROM dbo.watermark_control
WHERE table_name = 'olist_order_payments_dataset';
GO

UPDATE dbo.watermark_control
SET last_watermark = '1900-01-01', last_run_ts = NULL, last_row_count = NULL
GO

SELECT * FROM dbo.watermark_control
WHERE table_name = 'olist_order_payments_dataset';
GO
