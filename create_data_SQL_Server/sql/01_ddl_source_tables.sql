-- =============================================================================
-- File: 01_ddl_source_tables.sql
-- Project: MIGRATION-DBX-001
-- Description: DDL for all 9 Olist source tables + watermark infrastructure
-- Run on: migration-db (Azure SQL PaaS)
-- =============================================================================

USE [migration-db];
GO

-- ---------------------------------------------------------------------------
-- 1. olist_customers_dataset
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.olist_customers_dataset', 'U') IS NOT NULL
    DROP TABLE dbo.olist_customers_dataset;

CREATE TABLE dbo.olist_customers_dataset (
    customer_id             NVARCHAR(50)    NOT NULL,
    customer_unique_id      NVARCHAR(50)    NOT NULL,
    customer_zip_code_prefix NVARCHAR(10)   NULL,
    customer_city           NVARCHAR(100)   NULL,
    customer_state          NVARCHAR(2)     NULL,
    created_at              DATETIME2       NOT NULL DEFAULT SYSDATETIME(),  -- simulated watermark
    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);
GO

-- ---------------------------------------------------------------------------
-- 2. olist_geolocation_dataset
-- Note: No PK — legitimate duplicates per zip (we deduplicate in Silver)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.olist_geolocation_dataset', 'U') IS NOT NULL
    DROP TABLE dbo.olist_geolocation_dataset;

CREATE TABLE dbo.olist_geolocation_dataset (
    geolocation_zip_code_prefix NVARCHAR(10)    NOT NULL,
    geolocation_lat             FLOAT           NULL,
    geolocation_lng             FLOAT           NULL,
    geolocation_city            NVARCHAR(100)   NULL,
    geolocation_state           NVARCHAR(2)     NULL,
    created_at                  DATETIME2       NOT NULL DEFAULT SYSDATETIME()
);
GO

-- ---------------------------------------------------------------------------
-- 3. olist_sellers_dataset
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.olist_sellers_dataset', 'U') IS NOT NULL
    DROP TABLE dbo.olist_sellers_dataset;

CREATE TABLE dbo.olist_sellers_dataset (
    seller_id               NVARCHAR(50)    NOT NULL,
    seller_zip_code_prefix  NVARCHAR(10)    NULL,
    seller_city             NVARCHAR(100)   NULL,
    seller_state            NVARCHAR(2)     NULL,
    created_at              DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT pk_sellers PRIMARY KEY (seller_id)
);
GO

-- ---------------------------------------------------------------------------
-- 4. olist_products_dataset
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.olist_products_dataset', 'U') IS NOT NULL
    DROP TABLE dbo.olist_products_dataset;

CREATE TABLE dbo.olist_products_dataset (
    product_id                  NVARCHAR(50)    NOT NULL,
    product_category_name       NVARCHAR(100)   NULL,   -- Portuguese; ~600 rows NULL
    product_name_length         INT             NULL,
    product_description_length  INT             NULL,
    product_photos_qty          INT             NULL,
    product_weight_g            INT             NULL,
    product_length_cm           INT             NULL,
    product_height_cm           INT             NULL,
    product_width_cm            INT             NULL,
    created_at                  DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT pk_products PRIMARY KEY (product_id)
);
GO

-- ---------------------------------------------------------------------------
-- 5. product_category_name_translation (static lookup)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.product_category_name_translation', 'U') IS NOT NULL
    DROP TABLE dbo.product_category_name_translation;

CREATE TABLE dbo.product_category_name_translation (
    product_category_name         NVARCHAR(100)   NOT NULL,   -- Portuguese
    product_category_name_english NVARCHAR(100)   NOT NULL,   -- English
    CONSTRAINT pk_category_trans PRIMARY KEY (product_category_name)
);
GO

-- ---------------------------------------------------------------------------
-- 6. olist_orders_dataset  (main fact — no FK constraints; source is dirty)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.olist_orders_dataset', 'U') IS NOT NULL
    DROP TABLE dbo.olist_orders_dataset;

CREATE TABLE dbo.olist_orders_dataset (
    order_id                        NVARCHAR(50)    NOT NULL,
    customer_id                     NVARCHAR(50)    NULL,
    order_status                    NVARCHAR(20)    NULL,
    order_purchase_timestamp        DATETIME2       NULL,   -- PRIMARY watermark col
    order_approved_at               DATETIME2       NULL,
    order_delivered_carrier_date    DATETIME2       NULL,
    order_delivered_customer_date   DATETIME2       NULL,
    order_estimated_delivery_date   DATETIME2       NULL,
    CONSTRAINT pk_orders PRIMARY KEY (order_id)
);
GO

-- ---------------------------------------------------------------------------
-- 7. olist_order_items_dataset  (composite PK)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.olist_order_items_dataset', 'U') IS NOT NULL
    DROP TABLE dbo.olist_order_items_dataset;

CREATE TABLE dbo.olist_order_items_dataset (
    order_id            NVARCHAR(50)    NOT NULL,
    order_item_id       INT             NOT NULL,
    product_id          NVARCHAR(50)    NULL,
    seller_id           NVARCHAR(50)    NULL,
    shipping_limit_date DATETIME2       NULL,   -- watermark col
    price               DECIMAL(10,2)   NULL,
    freight_value       DECIMAL(10,2)   NULL,
    CONSTRAINT pk_order_items PRIMARY KEY (order_id, order_item_id)
);
GO

-- ---------------------------------------------------------------------------
-- 8. olist_order_payments_dataset  (composite PK)
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.olist_order_payments_dataset', 'U') IS NOT NULL
    DROP TABLE dbo.olist_order_payments_dataset;

CREATE TABLE dbo.olist_order_payments_dataset (
    order_id                NVARCHAR(50)    NOT NULL,
    payment_sequential      INT             NOT NULL,
    payment_type            NVARCHAR(20)    NULL,
    payment_installments    INT             NULL,
    payment_value           DECIMAL(10,2)   NULL,
    CONSTRAINT pk_order_payments PRIMARY KEY (order_id, payment_sequential)
);
GO

-- ---------------------------------------------------------------------------
-- 9. olist_order_reviews_dataset
-- Note: review_id not unique in raw data (same review_id, different answers)
-- We use review_id + order_id as logical key
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.olist_order_reviews_dataset', 'U') IS NOT NULL
    DROP TABLE dbo.olist_order_reviews_dataset;

CREATE TABLE dbo.olist_order_reviews_dataset (
    review_id               NVARCHAR(50)    NULL,   -- setting it as null just to have some usecases in the DQ checks; we won't use it as PK
    order_id                NVARCHAR(50)    NOT NULL,
    review_score            INT             NULL,
    review_comment_title    NVARCHAR(500)   NULL,   -- ~60% NULL in real data
    review_comment_message  NVARCHAR(MAX)   NULL,   -- ~60% NULL in real data
    review_creation_date    DATETIME2       NULL,   -- watermark col
    review_answer_timestamp DATETIME2       NULL
    -- No PK: review_id has duplicates in Olist raw data
    -- Silver layer handles deduplication
);
GO

-- =============================================================================
-- WATERMARK INFRASTRUCTURE
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Watermark control table
-- ---------------------------------------------------------------------------
IF OBJECT_ID('dbo.watermark_control', 'U') IS NOT NULL
    DROP TABLE dbo.watermark_control;

CREATE TABLE dbo.watermark_control (
    table_name          VARCHAR(100)    NOT NULL,
    watermark_col       VARCHAR(100)    NULL,       -- NULL for static tables (category_translation)
    last_watermark      DATETIME2       NOT NULL DEFAULT '1900-01-01 00:00:00',
    last_run_ts         DATETIME2       NULL,       -- when ADF last ran for this table
    last_row_count      BIGINT          NULL,       -- rows extracted last run
    CONSTRAINT pk_watermark PRIMARY KEY (table_name)
);
GO

-- Seed watermark values (all tables start at epoch — full load on first run)
INSERT INTO dbo.watermark_control (table_name, watermark_col, last_watermark)
VALUES
    ('olist_orders_dataset',              'order_purchase_timestamp',  '1900-01-01 00:00:00'),
    ('olist_order_items_dataset',         'shipping_limit_date',       '1900-01-01 00:00:00'),
    ('olist_order_payments_dataset',      'order_purchase_timestamp',  '1900-01-01 00:00:00'),
    ('olist_order_reviews_dataset',       'review_creation_date',      '1900-01-01 00:00:00'),
    ('olist_customers_dataset',           'created_at',                '1900-01-01 00:00:00'),
    ('olist_products_dataset',            'created_at',                '1900-01-01 00:00:00'),
    ('olist_sellers_dataset',             'created_at',                '1900-01-01 00:00:00'),
    ('olist_geolocation_dataset',         'created_at',                '1900-01-01 00:00:00'),
    ('product_category_name_translation', NULL,                        '1900-01-01 00:00:00');
GO

SELECT table_name, watermark_col, last_watermark FROM dbo.watermark_control ORDER BY table_name;
GO
