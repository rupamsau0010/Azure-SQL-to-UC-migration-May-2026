USE [migration-db];
GO

TRUNCATE TABLE dbo.olist_order_reviews_dataset;
SELECT count(*) AS row_count FROM dbo.olist_order_reviews_dataset;

--Show schema of the table
EXEC sp_help dbo.olist_order_reviews_dataset;