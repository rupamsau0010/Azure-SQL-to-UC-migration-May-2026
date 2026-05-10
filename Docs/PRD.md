# Project Requirements Document (PRD)
## Azure SQL PaaS → Databricks Unity Catalog Migration
**Project Code:** MIGRATION-DBX-001
**Version:** 1.0 | **Status:** Approved
**Author:** Rupam Sau | **Date:** May 2026
**Domain:** E-Commerce (Olist Brazilian E-Commerce Public Dataset)

---

## 1. Executive Summary

This project migrates Olist e-commerce transactional data from Azure SQL PaaS into a Databricks Lakehouse governed by Unity Catalog. Azure Data Factory orchestrates watermark-based incremental extraction into ADLS Gen2 (Bronze layer). Databricks jobs process data through Silver (DQ-validated Delta) and Gold (aggregated, star-schema Delta) layers. Full CI/CD is implemented via GitHub Actions and Databricks Asset Bundles. The project targets cost efficiency (< $15 total) while covering every real-world migration concern: incremental loading, data quality, secret management, idempotency, monitoring, and automated deployment.

---

## 2. Business Objectives

| ID | Objective |
|----|-----------|
| OBJ-01 | Centralize e-commerce data in a governed, scalable lakehouse platform |
| OBJ-02 | Demonstrate production-grade incremental load using ADF watermark pattern |
| OBJ-03 | Enforce structured data quality at Silver layer with quarantine for failed records |
| OBJ-04 | Establish repeatable, CI/CD-driven pipeline deployments via GitHub Actions |
| OBJ-05 | Practice Unity Catalog governance (grants, storage credentials, metastore) |
| OBJ-06 | Maximize real-world learning surface while keeping Azure spend under $15 |

---

## 3. Scope

### 3.1 In Scope
- All 9 Olist source tables loaded into Azure SQL PaaS
- Watermark-based incremental extraction via ADF
- Bronze layer: raw Parquet on ADLS Gen2, partitioned by `ingestion_date`
- Silver layer: Delta Lake — type casting, null handling, deduplication, 14 DQ rules
- Quarantine layer: Delta Lake — DQ-failed records with failure reason column
- Gold layer: Delta Lake — star schema dimensions + aggregated fact tables
- Unity Catalog: metastore setup, catalog, 4 schemas, column-level grants
- Azure Key Vault: all secrets, accessed via Managed Identity
- GitHub + GitHub Actions: repo structure, DAB deploy workflow
- ADF + Databricks monitoring and email alerting
- OPTIMIZE + ZORDER on high-query Gold tables
- Deliberate problem injection (by architect) for real-world debugging experience

### 3.2 Out of Scope
- Real-time streaming (Kafka / Event Hubs)
- Machine Learning / AI pipelines
- Power BI reporting (stretch goal only if time permits)
- Azure Purview / Microsoft Purview (cost)
- Multi-region or disaster recovery design
- Premium-tier Databricks features (column masking, row filters)

---

## 4. Source System Profile

| Attribute | Detail |
|-----------|--------|
| Platform | Azure SQL PaaS — General Purpose Basic tier (5 DTU) |
| Dataset | Olist Brazilian E-Commerce Public Dataset |
| Source | https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce |
| Number of Tables | 9 |
| Approximate Total Rows | ~800,000 across all tables |
| Data Sensitivity | Low (public dataset) |
| Load Strategy — Day 0 | Full load (all rows) |
| Load Strategy — Day 1+ | Watermark-based incremental (new/changed rows only) |
| Watermark Mechanism | `dbo.watermark_control` table in Azure SQL |
| Known Data Quality Issues | Nulls in reviews, duplicate geolocations, orphan order_items, Portuguese category names, timestamp inconsistencies |

### 4.1 Source Tables

| Table Name                        | Approx Rows | Natural Watermark Column                    | Notes                                |
|-----------------------------------|-------------|---------------------------------------------|--------------------------------------|
| olist_orders_dataset              | 99,441      | `order_purchase_timestamp`                  | Main fact table                      |
| olist_order_items_dataset         | 112,650     | `shipping_limit_date`                       | Multiple rows per order              |
| olist_order_payments_dataset      | 103,886     | `Derived via order join`                    | Multiple payments per order          |
| olist_order_reviews_dataset       | 99,224      | `review_creation_date`                      | Heavy nulls in comment fields        |
| olist_customers_dataset           | 99,441      | `created_at` (simulated - we add this column)|  No native timestamp                |
| olist_products_dataset            | 32,951      | `created_at` (simulated)                    | Portuguese category names            |
| olist_sellers_dataset             | 3,095       | `created_at` (simulated)                    | Small dimension                      |
| olist_geolocation_dataset         | 1,000,163   | `created_at` (simulated)                    | Massive duplication; largest table   |
| product_category_name_translation | 71          | `None` — static lookup                      | Join reference for category names    |

---

## 5. Target System Profile

| Attribute | Detail |
|-----------|--------|
| Platform | Azure Databricks — Standard tier |
| Databricks Runtime | DBR 14.3 LTS |
| Governance | Unity Catalog |
| Storage | ADLS Gen2 (Hierarchical Namespace enabled) |
| Bronze Format | Apache Parquet |
| Silver / Gold / Quarantine Format | Delta Lake |
| Unity Catalog Name | `migration_dev` |
| Schemas | `bronze`, `silver`, `gold`, `quarantine` |
| Cluster Type | Single-node (dev cost optimization) |
| Cluster VM | Standard_DS3_v2 (4 vCPU, 14 GB RAM) |
| Auto-terminate | 10 minutes idle |

---

## 6. Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-01 | ADF pipeline extracts all 9 tables from Azure SQL | Must Have |
| FR-02 | Watermark table (`dbo.watermark_control`) tracks last extracted max-timestamp per table | Must Have |
| FR-03 | ADF Copy Activity uses parameterized Lookup + ForEach to support all tables from single pipeline | Must Have |
| FR-04 | Bronze files land as Parquet on ADLS partitioned by `ingestion_date=YYYY-MM-DD` | Must Have |
| FR-05 | ADF Stored Procedure activity updates watermark after successful copy | Must Have |
| FR-06 | Databricks Bronze→Silver notebook: enforce schema, cast types, handle nulls, deduplicate | Must Have |
| FR-07 | 14 DQ rules applied at Silver layer (see Section 8) | Must Have |
| FR-08 | Records failing DQ written to `quarantine` schema with `dq_fail_reason` column | Must Have |
| FR-09 | Silver tables written as Delta with MERGE (upsert) pattern — ensures idempotency | Must Have |
| FR-10 | Databricks Silver→Gold notebook: build star schema + 3 aggregation tables | Must Have |
| FR-11 | All secrets (SQL conn string, ADLS key, SP secret) stored in Azure Key Vault | Must Have |
| FR-12 | Databricks accesses Key Vault via KV-backed secret scope (Managed Identity) | Must Have |
| FR-13 | ADF accesses Key Vault via ADF Managed Identity | Must Have |
| FR-14 | GitHub Actions workflow deploys Databricks jobs via DAB on push to `main` | Should Have |
| FR-15 | ADF pipeline runs on daily schedule trigger (02:00 UTC) | Should Have |
| FR-16 | Email alert on ADF pipeline failure | Should Have |
| FR-17 | Email alert on Databricks job failure | Should Have |
| FR-18 | OPTIMIZE + ZORDER applied to `fact_orders` and `agg_daily_revenue` Gold tables | Should Have |
| FR-19 | Unity Catalog grants applied: least-privilege per schema | Must Have |
| FR-20 | Pipeline is fully re-runnable (idempotent) — no duplicates on repeat run | Must Have |

---

## 7. Non-Functional Requirements

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-01 | Total Azure cost for entire project | < $15 |
| NFR-02 | Cluster idle auto-termination | 10 minutes |
| NFR-03 | End-to-end pipeline (full dataset, Day 0) | < 60 minutes |
| NFR-04 | End-to-end pipeline (incremental, Day 1+) | < 15 minutes |
| NFR-05 | Zero hardcoded credentials in any file | Strictly enforced |
| NFR-06 | All source code version-controlled in GitHub | Must |
| NFR-07 | Cluster type | Single-node (dev) |

---

## 8. Data Quality Framework (Silver Layer)

| Rule ID | Table | Check Type | Rule Description | Action on Fail |
|---------|-------|------------|------------------|----------------|
| DQ-01 | orders | Completeness | `order_id` IS NOT NULL | Quarantine |
| DQ-02 | orders | Validity | `order_status` IN ('delivered','shipped','canceled','unavailable','invoiced','processing','created','approved') | Quarantine |
| DQ-03 | orders | Timeliness | `order_purchase_timestamp` <= current_timestamp() | Quarantine |
| DQ-04 | orders | Referential | `customer_id` exists in silver.customers | Flag (dq_warn) |
| DQ-05 | order_items | Completeness | `order_id`, `product_id`, `seller_id` all NOT NULL | Quarantine |
| DQ-06 | order_items | Validity | `price` > 0 AND `freight_value` >= 0 | Quarantine |
| DQ-07 | order_payments | Validity | `payment_value` >= 0 | Quarantine |
| DQ-08 | order_payments | Validity | `payment_type` IN ('credit_card','boleto','voucher','debit_card','not_defined') | Flag |
| DQ-09 | customers | Completeness | `customer_id` IS NOT NULL | Quarantine |
| DQ-10 | customers | Uniqueness | No duplicate `customer_id` (keep latest by `created_at`) | Deduplicate |
| DQ-11 | order_reviews | Validity | `review_score` BETWEEN 1 AND 5 | Quarantine |
| DQ-12 | products | Completeness | `product_id` IS NOT NULL | Quarantine |
| DQ-13 | geolocation | Validity | `geolocation_lat` BETWEEN -90 AND 90 | Quarantine |
| DQ-14 | geolocation | Validity | `geolocation_lng` BETWEEN -180 AND 180 | Quarantine |

---

## 9. Gold Layer Targets

| Gold Table | Type | Source Silver Tables | Key Aggregation / Logic |
|------------|------|---------------------|-------------------------|
| `dim_customers` | Dimension (SCD1) | customers | Unique customers with geolocation joined |
| `dim_products` | Dimension (SCD1) | products, category_translation | English category name joined |
| `dim_sellers` | Dimension (SCD1) | sellers, geolocation | Seller location enriched |
| `dim_date` | Date Dimension | Generated | Calendar attributes (year, month, quarter, day_of_week) |
| `fact_orders` | Fact | orders, order_items, order_payments | Order-level grain, payment totals, item counts |
| `agg_daily_revenue` | Aggregate | fact_orders | Revenue, order count, avg order value by day |
| `agg_seller_performance` | Aggregate | fact_orders, dim_sellers | Revenue, review score, delivery time by seller |

---

## 10. Deliberately Injected Problems (Architect-Controlled)

These problems will be introduced at specific phases to simulate real-world debugging:

| Problem ID | When Injected | Description | Learning Outcome |
|------------|--------------|-------------|-----------------|
| PROB-01 | Phase 2 | Watermark not updating → full re-extract on incremental run | Debug ADF SP activity, idempotency with MERGE |
| PROB-02 | Phase 3 | Schema mismatch: `price` column arrives as STRING in one partition | Schema evolution, Bronze enforcement |
| PROB-03 | Phase 3 | 500 records injected with NULL `order_id` | DQ quarantine path validation |
| PROB-04 | Phase 3 | Duplicate `customer_id` records with different emails | Deduplication window function logic |
| PROB-05 | Phase 4 | Geolocation table has lat/lng swapped in 200 rows | Range DQ rule, quarantine investigation |
| PROB-06 | Phase 5 | Gold `fact_orders` has fanout (row explosion) due to bad join | Join grain analysis, DISTINCT vs GROUP BY |
| PROB-07 | Phase 6 | GitHub Actions DAB deploy fails — wrong target env | DAB bundle target config, GitHub Secrets |

---

## 11. Success Criteria

| Criterion | Measurement |
|-----------|-------------|
| All 9 tables migrated | Source row count = Silver row count + Quarantine row count |
| Zero unexplained data loss | Every missing row traceable to quarantine with reason |
| DQ rules functional | Injected bad records appear in correct quarantine table |
| Incremental load working | Day 1 extract contains only rows with timestamp > Day 0 watermark |
| Idempotency verified | Re-run same pipeline → row counts unchanged |
| CI/CD working | Push to `main` → GitHub Actions deploys DAB → Databricks job updated |
| UC governance | All tables visible in UC Explorer, grants applied per schema |
| Total cost | Azure spend < $15 |

---

## 12. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|-----------|
| $100 Azure credit exhausted | Low | High | Standard tier Databricks, Basic SQL, single-node cluster, aggressive auto-terminate |
| UC metastore misconfiguration | Medium | High | Follow exact setup sequence: storage account → access connector → metastore assignment |
| ADF watermark logic error → duplicate extraction | Medium | Medium | MERGE (not INSERT) into Silver Delta prevents duplicates even if watermark breaks |
| GitHub Actions Databricks auth failure | Low | Medium | Use Service Principal with Contributor role; store in GitHub Secrets |
| Geolocation table (1M rows) slow to process | Medium | Low | Filter deduplication at Bronze read; partition by zip prefix |
| Kaggle download requires account | Low | Low | Create free Kaggle account; download takes < 2 min |

---

*End of PRD. Version 1.0.*
