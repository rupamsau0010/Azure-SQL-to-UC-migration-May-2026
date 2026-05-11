-- =============================================================================
-- File: 03_verify_source_data.sql
-- Project: MIGRATION-DBX-001
-- Description: Post-load verification queries
-- Run AFTER load_olist_to_sql.py completes
-- =============================================================================

USE [migration-db];
GO

-- ---------------------------------------------------------------------------
-- 1. Row count check for all tables
-- ---------------------------------------------------------------------------
SELECT 'olist_customers_dataset'            AS table_name, COUNT(*) AS row_count, 99441    AS expected FROM dbo.olist_customers_dataset
UNION ALL
SELECT 'olist_geolocation_dataset',                        COUNT(*),              1000163   FROM dbo.olist_geolocation_dataset
UNION ALL
SELECT 'olist_sellers_dataset',                            COUNT(*),              3095      FROM dbo.olist_sellers_dataset
UNION ALL
SELECT 'olist_products_dataset',                           COUNT(*),              32951     FROM dbo.olist_products_dataset
UNION ALL
SELECT 'product_category_name_translation',                COUNT(*),              71        FROM dbo.product_category_name_translation
UNION ALL
SELECT 'olist_orders_dataset',                             COUNT(*),              99441     FROM dbo.olist_orders_dataset
UNION ALL
SELECT 'olist_order_items_dataset',                        COUNT(*),              112650    FROM dbo.olist_order_items_dataset
UNION ALL
SELECT 'olist_order_payments_dataset',                     COUNT(*),              103886    FROM dbo.olist_order_payments_dataset
UNION ALL
SELECT 'olist_order_reviews_dataset',                      COUNT(*),              99224     FROM dbo.olist_order_reviews_dataset
ORDER BY table_name;
GO

-- ---------------------------------------------------------------------------
-- 2. Watermark column sanity check
-- (created_at should span 2016–2018 for dim tables)
-- ---------------------------------------------------------------------------
SELECT 'customers' AS tbl,
    MIN(created_at) AS min_wm, MAX(created_at) AS max_wm,
    COUNT(*) AS rows
FROM dbo.olist_customers_dataset
UNION ALL
SELECT 'sellers',
    MIN(created_at), MAX(created_at), COUNT(*)
FROM dbo.olist_sellers_dataset
UNION ALL
SELECT 'products',
    MIN(created_at), MAX(created_at), COUNT(*)
FROM dbo.olist_products_dataset
UNION ALL
SELECT 'geolocation',
    MIN(created_at), MAX(created_at), COUNT(*)
FROM dbo.olist_geolocation_dataset
UNION ALL
SELECT 'orders (purchase_ts)',
    MIN(order_purchase_timestamp), MAX(order_purchase_timestamp), COUNT(*)
FROM dbo.olist_orders_dataset;
GO

-- ---------------------------------------------------------------------------
-- 3. Verify PROB-03: NULL review_ids in reviews table
-- (expect ~500)
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*)                                    AS total_reviews,
    SUM(CASE WHEN review_id IS NULL THEN 1 ELSE 0 END) AS null_review_ids,
    SUM(CASE WHEN review_score NOT BETWEEN 1 AND 5 THEN 1 ELSE 0 END) AS invalid_scores
FROM dbo.olist_order_reviews_dataset;
GO

-- ---------------------------------------------------------------------------
-- 4. Verify PROB-05: swapped lat/lng in geolocation
-- (expect ~200 rows where lat > 90 OR lat < -90)
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*) AS total_geo_rows,
    SUM(CASE WHEN geolocation_lat > 90 OR geolocation_lat < -90 THEN 1 ELSE 0 END) AS invalid_lat_rows,
    SUM(CASE WHEN geolocation_lng > 180 OR geolocation_lng < -180 THEN 1 ELSE 0 END) AS invalid_lng_rows
FROM dbo.olist_geolocation_dataset;
GO

-- ---------------------------------------------------------------------------
-- 5. Orders data quality preview
-- ---------------------------------------------------------------------------
SELECT order_status, COUNT(*) AS cnt
FROM dbo.olist_orders_dataset
GROUP BY order_status
ORDER BY cnt DESC;
GO

-- ---------------------------------------------------------------------------
-- 6. Multi-item orders (shows why GROUP BY order_id alone gives wrong revenue)
-- This previews PROB-06 you'll encounter in Gold phase
-- ---------------------------------------------------------------------------
SELECT TOP 10
    order_id,
    COUNT(*)        AS item_count,
    SUM(price)      AS total_price,
    SUM(freight_value) AS total_freight
FROM dbo.olist_order_items_dataset
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY item_count DESC;
GO

-- ---------------------------------------------------------------------------
-- 7. Products with NULL category (will join to 'unknown' in Gold)
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*) AS total_products,
    SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) AS null_category,
    COUNT(DISTINCT product_category_name) AS distinct_categories
FROM dbo.olist_products_dataset;
GO

-- ---------------------------------------------------------------------------
-- 8. Payment types distribution
-- ---------------------------------------------------------------------------
SELECT payment_type, COUNT(*) AS cnt, ROUND(AVG(payment_value),2) AS avg_value
FROM dbo.olist_order_payments_dataset
GROUP BY payment_type
ORDER BY cnt DESC;
GO

-- ---------------------------------------------------------------------------
-- 9. Orders with multiple payments (split payments — fanout risk)
-- ---------------------------------------------------------------------------
SELECT
    COUNT(DISTINCT order_id) AS orders_with_multiple_payments
FROM dbo.olist_order_payments_dataset
WHERE payment_sequential > 1;
GO

-- ---------------------------------------------------------------------------
-- 10. Watermark control table state (all should be 1900-01-01)
-- ---------------------------------------------------------------------------
SELECT table_name, watermark_col, last_watermark, last_run_ts, last_row_count
FROM dbo.watermark_control
ORDER BY table_name;
GO

-- =============================================================================
-- EXPECTED RESULTS SUMMARY (paste in your notes after running):
-- =============================================================================
-- customers:            99,441 rows  | created_at 2016-2018
-- geolocation:       1,000,163 rows  | ~200 invalid lat rows (PROB-05)
-- sellers:               3,095 rows  | created_at 2016-2018
-- products:             32,951 rows  | ~600 NULL category_name
-- category_translation:     71 rows
-- orders:               99,441 rows  | purchase_ts 2016-10 to 2018-10
-- order_items:         112,650 rows  | some orders have 20+ items
-- order_payments:      103,886 rows  | 3 payment types dominate
-- order_reviews:        99,224 rows  | ~500 NULL review_id (PROB-03)
-- watermark_control:        9 rows   | all last_watermark = 1900-01-01
-- =============================================================================
