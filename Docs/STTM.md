# Source-to-Target Mapping (STTM)
## Azure SQL PaaS → ADLS Gen2 → Databricks Unity Catalog
**Version:** 1.0 | **Date:** May 2026
**Project:** MIGRATION-DBX-001

---

## How to Read This Document

| Column | Meaning |
|--------|---------|
| Source Column | Column name in Azure SQL source table |
| Source Type | SQL Server data type |
| Bronze Column | Column name as landed in Parquet (Bronze) — no transformation |
| Bronze Type | Parquet inferred type |
| Silver Column | Column name in Silver Delta table |
| Silver Type | Spark / Delta type |
| Transformation | Logic applied Bronze → Silver |
| DQ Rule | Rule ID from DQ framework (see PRD Section 8) |
| Gold Usage | How this column is used in Gold layer |

**Bronze rule:** Land data exactly as-is from source. No renaming. No casting. Schema-on-read.
**Silver rule:** Apply all transformations, enforce types, deduplicate, quarantine DQ failures.
**Gold rule:** Aggregate, denormalize, model for analytics consumption.

---

## Table 1: olist_orders_dataset → silver.orders → gold.fact_orders

**Source:** `dbo.olist_orders_dataset`
**Bronze path:** `migration-raw/orders/ingestion_date=YYYY-MM-DD/`
**Silver table:** `migration_dev.silver.orders`
**Primary Key:** `order_id`
**Watermark column:** `order_purchase_timestamp`

### Column Mapping

| Source Column | Source Type | Bronze Column | Bronze Type | Silver Column | Silver Type | Transformation | DQ Rule |
|--------------|------------|--------------|------------|--------------|------------|---------------|---------|
| order_id | NVARCHAR(50) | order_id | STRING | order_id | STRING | TRIM, UPPER | DQ-01 (NOT NULL) |
| customer_id | NVARCHAR(50) | customer_id | STRING | customer_id | STRING | TRIM | DQ-04 (ref integrity) |
| order_status | NVARCHAR(20) | order_status | STRING | order_status | STRING | TRIM, LOWER | DQ-02 (valid values) |
| order_purchase_timestamp | DATETIME2 | order_purchase_timestamp | STRING | order_purchase_ts | TIMESTAMP | TO_TIMESTAMP, UTC normalize | DQ-03 (not future) |
| order_approved_at | DATETIME2 | order_approved_at | STRING | order_approved_at | TIMESTAMP | TO_TIMESTAMP; NULL allowed | — |
| order_delivered_carrier_date | DATETIME2 | order_delivered_carrier_date | STRING | order_delivered_carrier_ts | TIMESTAMP | TO_TIMESTAMP; NULL allowed | — |
| order_delivered_customer_date | DATETIME2 | order_delivered_customer_date | STRING | order_delivered_customer_ts | TIMESTAMP | TO_TIMESTAMP; NULL allowed | — |
| order_estimated_delivery_date | DATETIME2 | order_estimated_delivery_date | STRING | order_estimated_delivery_ts | TIMESTAMP | TO_TIMESTAMP; NULL allowed | — |
| *(ADF adds)* | — | ingestion_date | DATE | ingestion_date | DATE | Partition column; passed by ADF | — |
| *(Silver adds)* | — | — | — | silver_loaded_at | TIMESTAMP | current_timestamp() at write time | — |
| *(Silver adds)* | — | — | — | dq_warn_flag | BOOLEAN | TRUE if any DQ-04 warning triggered | — |

**Deduplication logic (Silver):**
```python
from pyspark.sql import Window
import pyspark.sql.functions as F

w = Window.partitionBy("order_id").orderBy(F.desc("order_purchase_ts"))
df_deduped = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
```

**MERGE key (Silver):** `order_id`

---

## Table 2: olist_order_items_dataset → silver.order_items → gold.fact_orders

**Source:** `dbo.olist_order_items_dataset`
**Bronze path:** `migration-raw/order_items/ingestion_date=YYYY-MM-DD/`
**Silver table:** `migration_dev.silver.order_items`
**Primary Key:** Composite (`order_id`, `order_item_id`)
**Watermark column:** `shipping_limit_date`

### Column Mapping

| Source Column | Source Type | Bronze Column | Bronze Type | Silver Column | Silver Type | Transformation | DQ Rule |
|--------------|------------|--------------|------------|--------------|------------|---------------|---------|
| order_id | NVARCHAR(50) | order_id | STRING | order_id | STRING | TRIM | DQ-05 (NOT NULL) |
| order_item_id | INT | order_item_id | LONG | order_item_id | INTEGER | CAST to INT | DQ-05 (NOT NULL) |
| product_id | NVARCHAR(50) | product_id | STRING | product_id | STRING | TRIM | DQ-05 (NOT NULL) |
| seller_id | NVARCHAR(50) | seller_id | STRING | seller_id | STRING | TRIM | DQ-05 (NOT NULL) |
| shipping_limit_date | DATETIME2 | shipping_limit_date | STRING | shipping_limit_ts | TIMESTAMP | TO_TIMESTAMP | — |
| price | DECIMAL(10,2) | price | DOUBLE | price | DECIMAL(10,2) | CAST; validate > 0 | DQ-06 |
| freight_value | DECIMAL(10,2) | freight_value | DOUBLE | freight_value | DECIMAL(10,2) | CAST; validate >= 0 | DQ-06 |
| *(Silver adds)* | — | — | — | total_item_value | DECIMAL(10,2) | `price + freight_value` | — |
| *(Silver adds)* | — | — | — | silver_loaded_at | TIMESTAMP | current_timestamp() | — |

**Real-world issue in Olist data:** Some orders have multiple items (order_item_id > 1). If you GROUP BY order_id without SUM, you get wrong revenue. This is injected as PROB-06 in Gold layer.

**MERGE key (Silver):** `order_id`, `order_item_id`

---

## Table 3: olist_order_payments_dataset → silver.order_payments → gold.fact_orders

**Source:** `dbo.olist_order_payments_dataset`
**Bronze path:** `migration-raw/order_payments/ingestion_date=YYYY-MM-DD/`
**Silver table:** `migration_dev.silver.order_payments`
**Primary Key:** Composite (`order_id`, `payment_sequential`)
**Watermark column:** Derived (join with orders on `order_purchase_timestamp` — payments have no native timestamp)

> **Note:** Because payments have no native timestamp, ADF uses a JOIN-based extraction: `SELECT p.* FROM order_payments p JOIN orders o ON p.order_id = o.order_id WHERE o.order_purchase_timestamp > @old_wm AND o.order_purchase_timestamp <= @new_wm`

### Column Mapping

| Source Column | Source Type | Bronze Column | Bronze Type | Silver Column | Silver Type | Transformation | DQ Rule |
|--------------|------------|--------------|------------|--------------|------------|---------------|---------|
| order_id | NVARCHAR(50) | order_id | STRING | order_id | STRING | TRIM | — |
| payment_sequential | INT | payment_sequential | LONG | payment_sequential | INTEGER | CAST | — |
| payment_type | NVARCHAR(20) | payment_type | STRING | payment_type | STRING | TRIM, LOWER | DQ-08 (valid values) |
| payment_installments | INT | payment_installments | LONG | payment_installments | INTEGER | CAST; NULL → 0 | — |
| payment_value | DECIMAL(10,2) | payment_value | DOUBLE | payment_value | DECIMAL(10,2) | CAST; validate >= 0 | DQ-07 |
| *(Silver adds)* | — | — | — | silver_loaded_at | TIMESTAMP | current_timestamp() | — |

**Real-world issue:** Multiple payment rows per order_id (split payments, installments). Fanout risk in Gold JOIN if not aggregated first.

**Aggregation before Gold join:**
```python
df_payments_agg = df_payments.groupBy("order_id").agg(
    F.sum("payment_value").alias("total_payment_value"),
    F.countDistinct("payment_type").alias("payment_type_count"),
    F.first("payment_type").alias("primary_payment_type"),
    F.max("payment_installments").alias("max_installments")
)
```

**MERGE key (Silver):** `order_id`, `payment_sequential`

---

## Table 4: olist_order_reviews_dataset → silver.order_reviews

**Source:** `dbo.olist_order_reviews_dataset`
**Bronze path:** `migration-raw/order_reviews/ingestion_date=YYYY-MM-DD/`
**Silver table:** `migration_dev.silver.order_reviews`
**Primary Key:** `review_id`
**Watermark column:** `review_creation_date`

### Column Mapping

| Source Column | Source Type | Bronze Column | Bronze Type | Silver Column | Silver Type | Transformation | DQ Rule |
|--------------|------------|--------------|------------|--------------|------------|---------------|---------|
| review_id | NVARCHAR(50) | review_id | STRING | review_id | STRING | TRIM | — |
| order_id | NVARCHAR(50) | order_id | STRING | order_id | STRING | TRIM | — |
| review_score | INT | review_score | LONG | review_score | INTEGER | CAST | DQ-11 (BETWEEN 1 AND 5) |
| review_comment_title | NVARCHAR(MAX) | review_comment_title | STRING | review_comment_title | STRING | TRIM; NULL allowed | — |
| review_comment_message | NVARCHAR(MAX) | review_comment_message | STRING | review_comment_message | STRING | TRIM; NULL allowed | — |
| review_creation_date | DATETIME2 | review_creation_date | STRING | review_creation_ts | TIMESTAMP | TO_TIMESTAMP | — |
| review_answer_timestamp | DATETIME2 | review_answer_timestamp | STRING | review_answer_ts | TIMESTAMP | TO_TIMESTAMP; NULL allowed | — |
| *(Silver adds)* | — | — | — | has_comment | BOOLEAN | `review_comment_message IS NOT NULL` | — |
| *(Silver adds)* | — | — | — | silver_loaded_at | TIMESTAMP | current_timestamp() | — |

**Real-world issue in Olist:** ~60% of reviews have NULL comment message. DQ-11 catches invalid scores but NULLs in comments are expected and allowed.

**MERGE key (Silver):** `review_id`

---

## Table 5: olist_customers_dataset → silver.customers → gold.dim_customers

**Source:** `dbo.olist_customers_dataset`
**Bronze path:** `migration-raw/customers/ingestion_date=YYYY-MM-DD/`
**Silver table:** `migration_dev.silver.customers`
**Primary Key:** `customer_id`
**Watermark column:** `created_at` (simulated — we add this column in SQL with DEFAULT GETDATE())

### Column Mapping

| Source Column | Source Type | Bronze Column | Bronze Type | Silver Column | Silver Type | Transformation | DQ Rule |
|--------------|------------|--------------|------------|--------------|------------|---------------|---------|
| customer_id | NVARCHAR(50) | customer_id | STRING | customer_id | STRING | TRIM | DQ-09 (NOT NULL) |
| customer_unique_id | NVARCHAR(50) | customer_unique_id | STRING | customer_unique_id | STRING | TRIM | — |
| customer_zip_code_prefix | NVARCHAR(10) | customer_zip_code_prefix | STRING | customer_zip_code_prefix | STRING | LPAD to 5 chars | — |
| customer_city | NVARCHAR(100) | customer_city | STRING | customer_city | STRING | TRIM, LOWER, normalize encoding | — |
| customer_state | NVARCHAR(2) | customer_state | STRING | customer_state | STRING | TRIM, UPPER | — |
| created_at | DATETIME2 | created_at | STRING | created_at | TIMESTAMP | TO_TIMESTAMP | — |
| *(Silver adds)* | — | — | — | silver_loaded_at | TIMESTAMP | current_timestamp() | — |

**Real-world issue:** `customer_city` has encoding artifacts from Portuguese (e.g., "sao paulo" vs "são paulo"). Normalize with LOWER + strip accents in Silver.

**Deduplication:** DQ-10 — if duplicate `customer_id` exists, keep row with latest `created_at`.

**MERGE key (Silver):** `customer_id`

---

## Table 6: olist_products_dataset → silver.products → gold.dim_products

**Source:** `dbo.olist_products_dataset`
**Bronze path:** `migration-raw/products/ingestion_date=YYYY-MM-DD/`
**Silver table:** `migration_dev.silver.products`
**Primary Key:** `product_id`
**Watermark column:** `created_at` (simulated)

### Column Mapping

| Source Column | Source Type | Bronze Column | Bronze Type | Silver Column | Silver Type | Transformation | DQ Rule |
|--------------|------------|--------------|------------|--------------|------------|---------------|---------|
| product_id | NVARCHAR(50) | product_id | STRING | product_id | STRING | TRIM | DQ-12 (NOT NULL) |
| product_category_name | NVARCHAR(100) | product_category_name | STRING | product_category_name_pt | STRING | Keep Portuguese original | — |
| product_name_length | INT | product_name_length | LONG | product_name_length | INTEGER | CAST; NULL allowed | — |
| product_description_length | INT | product_description_length | LONG | product_description_length | INTEGER | CAST; NULL allowed | — |
| product_photos_qty | INT | product_photos_qty | LONG | product_photos_qty | INTEGER | CAST; NULL → 0 | — |
| product_weight_g | INT | product_weight_g | LONG | product_weight_g | INTEGER | CAST; NULL allowed | — |
| product_length_cm | INT | product_length_cm | LONG | product_length_cm | INTEGER | CAST; NULL allowed | — |
| product_height_cm | INT | product_height_cm | LONG | product_height_cm | INTEGER | CAST; NULL allowed | — |
| product_width_cm | INT | product_width_cm | LONG | product_width_cm | INTEGER | CAST; NULL allowed | — |
| created_at | DATETIME2 | created_at | STRING | created_at | TIMESTAMP | TO_TIMESTAMP | — |
| *(Silver adds)* | — | — | — | silver_loaded_at | TIMESTAMP | current_timestamp() | — |

**Real-world issue:** ~600 products have NULL `product_category_name`. English translation joined at Gold layer from `category_translation` table.

**MERGE key (Silver):** `product_id`

---

## Table 7: olist_sellers_dataset → silver.sellers → gold.dim_sellers

**Source:** `dbo.olist_sellers_dataset`
**Bronze path:** `migration-raw/sellers/ingestion_date=YYYY-MM-DD/`
**Silver table:** `migration_dev.silver.sellers`
**Primary Key:** `seller_id`
**Watermark column:** `created_at` (simulated)

### Column Mapping

| Source Column | Source Type | Bronze Column | Bronze Type | Silver Column | Silver Type | Transformation | DQ Rule |
|--------------|------------|--------------|------------|--------------|------------|---------------|---------|
| seller_id | NVARCHAR(50) | seller_id | STRING | seller_id | STRING | TRIM | — |
| seller_zip_code_prefix | NVARCHAR(10) | seller_zip_code_prefix | STRING | seller_zip_code_prefix | STRING | LPAD to 5 chars | — |
| seller_city | NVARCHAR(100) | seller_city | STRING | seller_city | STRING | TRIM, LOWER, normalize | — |
| seller_state | NVARCHAR(2) | seller_state | STRING | seller_state | STRING | TRIM, UPPER | — |
| created_at | DATETIME2 | created_at | STRING | created_at | TIMESTAMP | TO_TIMESTAMP | — |
| *(Silver adds)* | — | — | — | silver_loaded_at | TIMESTAMP | current_timestamp() | — |

**MERGE key (Silver):** `seller_id`

---

## Table 8: olist_geolocation_dataset → silver.geolocation

**Source:** `dbo.olist_geolocation_dataset`
**Bronze path:** `migration-raw/geolocation/ingestion_date=YYYY-MM-DD/`
**Silver table:** `migration_dev.silver.geolocation`
**Primary Key:** None (deduplicate by `geolocation_zip_code_prefix` to get 1 row per zip)
**Watermark column:** `created_at` (simulated)
**Warning:** 1,000,163 rows — largest table. Heavy duplicate rate (~95% duplicates per zip).

### Column Mapping

| Source Column | Source Type | Bronze Column | Bronze Type | Silver Column | Silver Type | Transformation | DQ Rule |
|--------------|------------|--------------|------------|--------------|------------|---------------|---------|
| geolocation_zip_code_prefix | NVARCHAR(10) | geolocation_zip_code_prefix | STRING | geolocation_zip_code_prefix | STRING | LPAD to 5 chars | — |
| geolocation_lat | FLOAT | geolocation_lat | DOUBLE | geolocation_lat | DOUBLE | Validate range | DQ-13 |
| geolocation_lng | FLOAT | geolocation_lng | DOUBLE | geolocation_lng | DOUBLE | Validate range | DQ-14 |
| geolocation_city | NVARCHAR(100) | geolocation_city | STRING | geolocation_city | STRING | TRIM, LOWER | — |
| geolocation_state | NVARCHAR(2) | geolocation_state | STRING | geolocation_state | STRING | TRIM, UPPER | — |
| created_at | DATETIME2 | created_at | STRING | created_at | TIMESTAMP | TO_TIMESTAMP | — |
| *(Silver adds)* | — | — | — | silver_loaded_at | TIMESTAMP | current_timestamp() | — |

**Deduplication strategy (critical for this table):**
```python
# Keep 1 row per zip — use median lat/lng to avoid outlier coordinates
from pyspark.sql import Window
import pyspark.sql.functions as F

df_geo_deduped = df_geo_valid.groupBy("geolocation_zip_code_prefix").agg(
    F.percentile_approx("geolocation_lat", 0.5).alias("geolocation_lat"),
    F.percentile_approx("geolocation_lng", 0.5).alias("geolocation_lng"),
    F.first("geolocation_city").alias("geolocation_city"),
    F.first("geolocation_state").alias("geolocation_state")
)
```

**Real-world issue:** PROB-05 — 200 rows have lat/lng swapped (lat is > 90, lng is < -90). DQ-13 and DQ-14 catch these.

**MERGE key (Silver):** `geolocation_zip_code_prefix` (1 row per zip after dedup)

---

## Table 9: product_category_name_translation → silver.category_translation

**Source:** `dbo.product_category_name_translation`
**Bronze path:** `migration-raw/category_translation/ingestion_date=YYYY-MM-DD/`
**Silver table:** `migration_dev.silver.category_translation`
**Primary Key:** `product_category_name` (Portuguese)
**Watermark column:** None — static lookup. ADF does full load every run (71 rows, negligible cost).

### Column Mapping

| Source Column | Source Type | Bronze Column | Bronze Type | Silver Column | Silver Type | Transformation |
|--------------|------------|--------------|------------|--------------|------------|---------------|
| product_category_name | NVARCHAR(100) | product_category_name | STRING | product_category_name_pt | STRING | TRIM, LOWER |
| product_category_name_english | NVARCHAR(100) | product_category_name_english | STRING | product_category_name_en | STRING | TRIM, LOWER, replace underscore with space |

**MERGE key (Silver):** `product_category_name_pt`

---

## Gold Layer Mappings

### gold.dim_customers

| Gold Column | Source (Silver) | Transformation |
|-------------|----------------|---------------|
| customer_sk | Surrogate | `monotonically_increasing_id()` or hash of `customer_id` |
| customer_id | silver.customers.customer_id | As-is |
| customer_unique_id | silver.customers.customer_unique_id | As-is |
| customer_city | silver.customers.customer_city | As-is (normalized in Silver) |
| customer_state | silver.customers.customer_state | As-is |
| customer_zip_code_prefix | silver.customers.customer_zip_code_prefix | As-is |
| geolocation_lat | silver.geolocation.geolocation_lat | JOIN on zip_code_prefix |
| geolocation_lng | silver.geolocation.geolocation_lng | JOIN on zip_code_prefix |
| gold_loaded_at | Generated | current_timestamp() |

---

### gold.dim_products

| Gold Column | Source (Silver) | Transformation |
|-------------|----------------|---------------|
| product_sk | Surrogate | hash of `product_id` |
| product_id | silver.products.product_id | As-is |
| product_category_name_pt | silver.products.product_category_name_pt | As-is |
| product_category_name_en | silver.category_translation.product_category_name_en | LEFT JOIN on pt name; NULL → 'unknown' |
| product_weight_g | silver.products.product_weight_g | As-is |
| product_photos_qty | silver.products.product_photos_qty | As-is |
| gold_loaded_at | Generated | current_timestamp() |

---

### gold.dim_sellers

| Gold Column | Source (Silver) | Transformation |
|-------------|----------------|---------------|
| seller_sk | Surrogate | hash of `seller_id` |
| seller_id | silver.sellers.seller_id | As-is |
| seller_city | silver.sellers.seller_city | As-is |
| seller_state | silver.sellers.seller_state | As-is |
| seller_zip_code_prefix | silver.sellers.seller_zip_code_prefix | As-is |
| geolocation_lat | silver.geolocation.geolocation_lat | JOIN on zip_code_prefix |
| geolocation_lng | silver.geolocation.geolocation_lng | JOIN on zip_code_prefix |
| gold_loaded_at | Generated | current_timestamp() |

---

### gold.dim_date

| Gold Column | Derivation |
|-------------|-----------|
| date_key | INTEGER: YYYYMMDD format |
| full_date | DATE |
| year | YEAR(full_date) |
| quarter | QUARTER(full_date) |
| month | MONTH(full_date) |
| month_name | DATE_FORMAT(full_date, 'MMMM') |
| week_of_year | WEEKOFYEAR(full_date) |
| day_of_week | DAYOFWEEK(full_date) |
| day_name | DATE_FORMAT(full_date, 'EEEE') |
| is_weekend | BOOLEAN: day_of_week IN (1, 7) |

**Generated:** date spine from `2016-01-01` to `2020-12-31` (covers Olist dataset range)

---

### gold.fact_orders

| Gold Column | Source (Silver) | Transformation |
|-------------|----------------|---------------|
| order_sk | Surrogate | hash of `order_id` |
| order_id | silver.orders.order_id | As-is |
| customer_sk | gold.dim_customers | JOIN on customer_id → get SK |
| date_key | gold.dim_date | CAST(order_purchase_ts, 'yyyyMMdd') |
| order_status | silver.orders.order_status | As-is |
| order_purchase_ts | silver.orders.order_purchase_ts | As-is |
| order_delivered_customer_ts | silver.orders.order_delivered_customer_ts | As-is |
| delivery_days | Derived | `DATEDIFF(day, order_purchase_ts, order_delivered_customer_ts)` |
| total_item_value | silver.order_items | SUM(price) per order_id |
| total_freight_value | silver.order_items | SUM(freight_value) per order_id |
| total_payment_value | silver.order_payments | SUM(payment_value) per order_id |
| item_count | silver.order_items | COUNT(order_item_id) per order_id |
| primary_payment_type | silver.order_payments | First payment_type after aggregation |
| avg_review_score | silver.order_reviews | AVG(review_score) per order_id |
| gold_loaded_at | Generated | current_timestamp() |

**MERGE key (Gold):** `order_id`
**OPTIMIZE + ZORDER:** `ZORDER BY (order_purchase_ts, customer_sk)` — common filter columns

---

### gold.agg_daily_revenue

| Gold Column | Source | Transformation |
|-------------|--------|---------------|
| agg_date | gold.fact_orders.order_purchase_ts | CAST to DATE |
| order_count | gold.fact_orders | COUNT(order_id) |
| total_revenue | gold.fact_orders | SUM(total_payment_value) |
| avg_order_value | gold.fact_orders | AVG(total_payment_value) |
| total_items_sold | gold.fact_orders | SUM(item_count) |
| avg_delivery_days | gold.fact_orders | AVG(delivery_days) WHERE delivered |
| gold_loaded_at | Generated | current_timestamp() |

**MERGE key:** `agg_date`
**OPTIMIZE + ZORDER:** `ZORDER BY (agg_date)`

---

### gold.agg_seller_performance

| Gold Column | Source | Transformation |
|-------------|--------|---------------|
| seller_sk | gold.dim_sellers | As-is |
| seller_id | gold.dim_sellers | As-is |
| seller_state | gold.dim_sellers | As-is |
| total_orders | gold.fact_orders + order_items | COUNT DISTINCT order_id |
| total_revenue | gold.fact_orders + order_items | SUM(price) by seller |
| avg_review_score | gold.fact_orders | AVG(avg_review_score) |
| avg_delivery_days | gold.fact_orders | AVG(delivery_days) |
| gold_loaded_at | Generated | current_timestamp() |

**MERGE key:** `seller_id`

---

## Quarantine Schema

Every Silver table has a corresponding quarantine table with the same columns PLUS:

| Extra Column | Type | Description |
|-------------|------|-------------|
| `dq_rule_id` | STRING | Rule that failed (e.g., 'DQ-01') |
| `dq_fail_reason` | STRING | Human-readable: "order_id is NULL" |
| `source_table` | STRING | Which source table this record came from |
| `ingestion_date` | DATE | When the Bronze file was ingested |
| `quarantine_loaded_at` | TIMESTAMP | When written to quarantine |

Example quarantine record:
```
order_id:           NULL
customer_id:        abc123
order_status:       delivered
dq_rule_id:         DQ-01
dq_fail_reason:     order_id is NULL — record cannot be keyed
source_table:       orders
ingestion_date:     2026-05-10
quarantine_loaded_at: 2026-05-10T03:15:00Z
```

---

## ADF Watermark Control Seed Data

Initial state (Day 0 — before any run):

```sql
INSERT INTO dbo.watermark_control (table_name, watermark_col, last_watermark) VALUES
('olist_orders_dataset',              'order_purchase_timestamp',  '1900-01-01'),
('olist_order_items_dataset',         'shipping_limit_date',       '1900-01-01'),
('olist_order_payments_dataset',      'order_purchase_timestamp',  '1900-01-01'),
('olist_order_reviews_dataset',       'review_creation_date',      '1900-01-01'),
('olist_customers_dataset',           'created_at',                '1900-01-01'),
('olist_products_dataset',            'created_at',                '1900-01-01'),
('olist_sellers_dataset',             'created_at',                '1900-01-01'),
('olist_geolocation_dataset',         'created_at',                '1900-01-01'),
('product_category_name_translation', NULL,                        '1900-01-01');
-- Last table: static, always full load. Watermark not used but row needed for ForEach.
```

---

*End of STTM. Version 1.0.*
