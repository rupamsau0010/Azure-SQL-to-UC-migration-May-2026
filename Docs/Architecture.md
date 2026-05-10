# Architecture Document
## Azure SQL PaaS → Databricks Unity Catalog Migration
**Version:** 1.0 | **Status:** Approved
**Author:** Rupam Sau | **Date:** May 2026

---

## 1. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE LAYER                                    │
│                                                                              │
│   ┌──────────────────────────────────────────────────────────────────────┐   │
│   │                   Azure SQL PaaS (Basic Tier, 5 DTU)                 │   │
│   │                                                                      │   │
│   │  ┌────────────────┐  ┌─────────────────────────────────────────────┐ │   │
│   │  │ dbo.watermark  │  │  9 Source Tables (Olist E-Commerce)         │ │   │
│   │  │ _control       │  │  orders | order_items | order_payments      │ │   │
│   │  │ (watermark     │  │  order_reviews | customers | products       │ │   │
│   │  │  tracking)     │  │  sellers | geolocation | category_trans     │ │   │
│   │  └────────────────┘  └─────────────────────────────────────────────┘ │   │
│   └──────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────┬──────────────────────────────────────────────┘
                                │
                    ADF Linked Service (MSI → KV → SQL conn string)
                    Copy Activity — WHERE ts > watermark AND ts <= new_wm
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATION LAYER                                  │
│                                                                              │
│   ┌──────────────────────────────────────────────────────────────────────┐   │
│   │                    Azure Data Factory                                │   │
│   │                                                                      │   │
│   │   Pipeline: pl_incremental_ingest                                    │   │
│   │   ┌──────────┐   ┌──────────┐   ┌──────────────┐   ┌────────────┐    │   │
│   │   │ Lookup   │ → │ Lookup   │ → │  ForEach     │ → │ Stored     │    │   │
│   │   │ Get Old  │   │ Get New  │   │  (each table)│   │ Procedure  │    │   │
│   │   │Watermark │   │Watermark │   │  Copy to     │   │ Update     │    │   │
│   │   │          │   │(MAX ts)  │   │  Bronze      │   │ Watermark  │    │   │
│   │   └──────────┘   └──────────┘   └──────────────┘   └────────────┘    │   │
│   │                                                                      │   │
│   │   Trigger: ScheduleTrigger — daily 02:00 UTC                         │   │
│   │   Monitoring: Alert on failure → Email                               │   │
│   └──────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────┬──────────────────────────────────────────────┘
                                │
                    Parquet files, partitioned by ingestion_date=YYYY-MM-DD
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER — ADLS Gen2                            │
│                  (Hierarchical Namespace Enabled)                            │
│                                                                              │
│   Storage Account: migrationadls<suffix>                                     │
│                                                                              │
│   ┌───────────────────────────────────────────────────────────────────────┐  │
│   │  Container: migration-raw  (Bronze)                                   │  │
│   │  ├── orders/ingestion_date=2026-05-10/part-00000.parquet              │  │
│   │  ├── order_items/ingestion_date=2026-05-10/part-00000.parquet         │  │
│   │  ├── customers/ingestion_date=2026-05-10/part-00000.parquet           │  │
│   │  └── ... (all 9 tables, new partition per daily run)                  │  │
│   └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│   ┌───────────────────────────────────────────────────────────────────────┐  │
│   │  Container: migration-processed  (Silver + Gold)                      │  │
│   │  ├── silver/orders/_delta_log/ + parquet files                        │  │
│   │  ├── silver/customers/...                                             │  │
│   │  ├── gold/dim_customers/...                                           │  │
│   │  └── gold/fact_orders/...                                             │  │
│   └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│   ┌───────────────────────────────────────────────────────────────────────┐  │
│   │  Container: migration-quarantine                                      │  │
│   │  └── (DQ-failed records per table, Delta format)                      │  │
│   └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│   ┌───────────────────────────────────────────────────────────────────────┐  │
│   │  Container: unity-catalog-metastore  (UC managed storage)             │  │
│   │  └── (Unity Catalog internal metadata — do not touch manually)        │  │
│   └───────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────┬──────────────────────────────────────────────┘
                                │
              Databricks Auto Loader reads Bronze → processes → writes Silver/Gold
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       PROCESSING & GOVERNANCE LAYER                          │
│                                                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐    │
│   │              Azure Databricks (Standard Tier)                       │    │
│   │                                                                     │    │
│   │  Cluster: Single-node | Standard_DS3_v2 | DBR 14.3 LTS              │    │
│   │  Auto-terminate: 10 min idle                                        │    │
│   │                                                                     │    │
│   │  ┌──────────────────────────────────────────────────────────────┐   │    │
│   │  │  Databricks Job 1: job_bronze_to_silver                      │   │    │
│   │  │  Notebook: 01_bronze_to_silver.py                            │   │    │
│   │  │  ┌───────────────────────────────────────────────────────┐   │   │    │
│   │  │  │ 1. Read Parquet from Bronze (incremental partition)   │   │   │    │
│   │  │  │ 2. Enforce schema + cast types                        │   │   │    │
│   │  │  │ 3. Deduplication (window function, keep latest)       │   │   │    │
│   │  │  │ 4. Apply DQ rules (14 checks)                         │   │   │    │
│   │  │  │ 5. Pass records → MERGE into Silver Delta             │   │   │    │
│   │  │  │ 6. Fail records → APPEND to Quarantine Delta          │   │   │    │
│   │  │  └───────────────────────────────────────────────────────┘   │   │    │
│   │  └──────────────────────────────────────────────────────────────┘   │    │
│   │                                                                     │    │
│   │  ┌──────────────────────────────────────────────────────────────┐   │    │
│   │  │  Databricks Job 2: job_silver_to_gold                        │   │    │
│   │  │  Notebook: 02_silver_to_gold.py                              │   │    │
│   │  │  ┌───────────────────────────────────────────────────────┐   │   │    │
│   │  │  │ 1. Build dim_customers, dim_products, dim_sellers     │   │   │    │
│   │  │  │ 2. Build dim_date (calendar spine)                    │   │   │    │
│   │  │  │ 3. Build fact_orders (join dims, resolve keys)        │   │   │    │
│   │  │  │ 4. Build agg_daily_revenue                            │   │   │    │
│   │  │  │ 5. Build agg_seller_performance                       │   │   │    │
│   │  │  │ 6. OPTIMIZE + ZORDER (fact_orders, agg_daily_revenue) │   │   │    │
│   │  │  └───────────────────────────────────────────────────────┘   │   │    │
│   │  └──────────────────────────────────────────────────────────────┘   │    │
│   │                                                                     │    │
│   │  ┌──────────────────────────────────────────────────────────────┐   │    │
│   │  │              Unity Catalog                                   │   │    │
│   │  │  Metastore: migration-metastore (1 per Azure region)         │   │    │
│   │  │  Catalog:   migration_dev                                    │   │    │
│   │  │  Schemas:   bronze | silver | gold | quarantine              │   │    │
│   │  │  Storage Credential: MSI-backed (Databricks Access Connector)│   │    │
│   │  │  External Locations: mapped to ADLS containers               │   │    │
│   │  │  Grants: USE CATALOG, USE SCHEMA, SELECT (per schema)        │   │    │
│   │  └──────────────────────────────────────────────────────────────┘   │    │
│   └─────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                         SECURITY LAYER                                       │
│                                                                              │
│   Azure Key Vault (migration-kv)                                             │
│   ├── Secret: sql-connection-string                                          │
│   ├── Secret: adls-account-key                                               │
│   └── Secret: databricks-sp-client-secret                                    │
│                                                                              │
│   Access:                                                                    │
│   ADF Managed Identity ──────────────► Key Vault (Get, List)                 │
│   Databricks Access Connector (MSI) ─► Key Vault (Get, List)                 │
│   Databricks KV-backed Secret Scope ─► KV secrets used in notebooks          │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                          CI/CD LAYER                                         │
│                                                                              │
│   GitHub Repository: migration-dbx-project                                   │
│   Branch strategy: main (prod) | dev (feature branches)                      │
│                                                                              │
│   GitHub Actions Workflow: deploy-databricks.yml                             │
│   Trigger: push to main                                                      │
│   Steps:                                                                     │
│   1. Checkout repo                                                           │
│   2. Setup Databricks CLI (v0.220+)                                          │
│   3. databricks bundle validate                                              │
│   4. databricks bundle deploy --target dev                                   │
│   5. (Optional) databricks bundle run job_bronze_to_silver                   │
│                                                                              │
│   Auth: Service Principal (AZURE_SP_CLIENT_ID, AZURE_SP_CLIENT_SECRET,       │
│          AZURE_TENANT_ID, DATABRICKS_HOST) → GitHub Secrets                  │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Component Deep-Dive

### 2.1 Azure SQL PaaS

| Attribute | Value |
|-----------|-------|
| Tier | Basic (5 DTU, 2 GB max) |
| Region | Same as all other resources (East US or UK South) |
| Purpose | Source system simulation |
| Auth | SQL Authentication; password stored in Key Vault |
| Schema | `dbo` — all 9 tables + watermark control table |

**Watermark Control Table:**
```sql
CREATE TABLE dbo.watermark_control (
    table_name      VARCHAR(100) PRIMARY KEY,
    watermark_col   VARCHAR(100) NOT NULL,
    last_watermark  DATETIME2   NOT NULL DEFAULT '1900-01-01'
);
```

**Stored Procedure (called by ADF post-copy):**
```sql
CREATE PROCEDURE dbo.usp_update_watermark
    @table_name    VARCHAR(100),
    @new_watermark DATETIME2
AS
BEGIN
    UPDATE dbo.watermark_control
    SET last_watermark = @new_watermark
    WHERE table_name = @table_name;
END
```

---

### 2.2 Azure Data Factory

**Pipeline: `pl_incremental_ingest`**

| Activity | Type | Description |
|----------|------|-------------|
| `act_get_old_watermark` | Lookup | Reads `last_watermark` from `dbo.watermark_control` for given table |
| `act_get_new_watermark` | Lookup | Runs `SELECT MAX(watermark_col) FROM source_table` |
| `act_foreach_tables` | ForEach | Iterates over table config array; calls inner copy pipeline |
| `act_copy_to_bronze` | Copy | Extracts rows WHERE wm_col > old_wm AND wm_col <= new_wm; writes Parquet to ADLS |
| `act_update_watermark` | Stored Procedure | Calls `usp_update_watermark` with new_wm value |

**Linked Services:**
- `ls_azure_sql` — Azure SQL via KV-referenced connection string
- `ls_adls_gen2` — ADLS Gen2 via Managed Identity (no keys in ADF)

**Datasets (parameterized):**
- `ds_sql_source` — Parameters: `table_name`, `watermark_col`, `old_wm`, `new_wm`
- `ds_adls_bronze_parquet` — Parameters: `table_name`, `ingestion_date`

---

### 2.3 ADLS Gen2

| Container | Purpose | Format | Access |
|-----------|---------|--------|--------|
| `migration-raw` | Bronze — raw ingested files | Parquet | ADF (write), Databricks (read) |
| `migration-processed` | Silver + Gold Delta tables | Delta Lake | Databricks (read/write) |
| `migration-quarantine` | DQ-failed records | Delta Lake | Databricks (write), analyst (read) |
| `unity-catalog-metastore` | UC managed metadata storage | Internal | Databricks Access Connector only |

**Bronze partition structure:**
```
migration-raw/
└── <table_name>/
    └── ingestion_date=YYYY-MM-DD/
        └── part-00000-<uuid>.snappy.parquet
```

---

### 2.4 Azure Databricks — Cluster Config

```json
{
  "cluster_name": "migration-dev-cluster",
  "spark_version": "14.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 0,
  "spark_conf": {
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*]"
  },
  "autotermination_minutes": 10,
  "data_security_mode": "SINGLE_USER"
}
```

> **Why single-node?** Cost. Standard_DS3_v2 = ~$0.19/hr DBU for Standard tier. Two hours of dev work per day = ~$0.38/day. Cluster can still run PySpark — single-node just uses local[*] master.

---

### 2.5 Unity Catalog Setup Sequence

> **Critical:** UC setup must follow this exact order. Out-of-order steps cause hard-to-debug errors.

```
Step 1: Create ADLS container: unity-catalog-metastore
Step 2: Create Databricks Access Connector (managed identity resource in Azure)
Step 3: Assign "Storage Blob Data Contributor" role:
        Access Connector MSI → unity-catalog-metastore container
Step 4: In Databricks Account Console → Metastores → Create metastore
        (point to unity-catalog-metastore container, assign Access Connector)
Step 5: Assign metastore to Databricks workspace
Step 6: In workspace → Catalog → Create catalog: migration_dev
Step 7: Create schemas: bronze, silver, gold, quarantine
Step 8: Create External Location for migration-raw and migration-processed containers
Step 9: Grant permissions (see below)
```

**Unity Catalog Grant Pattern:**
```sql
-- Catalog level
GRANT USE CATALOG ON CATALOG migration_dev TO `your_email@domain.com`;

-- Schema level
GRANT USE SCHEMA ON SCHEMA migration_dev.bronze TO `your_email@domain.com`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA migration_dev.silver TO `your_email@domain.com`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA migration_dev.gold TO `your_email@domain.com`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA migration_dev.quarantine TO `your_email@domain.com`;

-- Table level (after tables created)
GRANT SELECT ON TABLE migration_dev.gold.fact_orders TO `your_email@domain.com`;
```

---

### 2.6 Azure Key Vault

| Secret Name | Value | Accessed By |
|-------------|-------|-------------|
| `sql-connection-string` | Full JDBC connection string for Azure SQL | ADF (via MSI), Databricks (via secret scope) |
| `adls-account-name` | Storage account name | Databricks |
| `adls-account-key` | ADLS Gen2 access key | Databricks (via secret scope) |
| `databricks-sp-client-id` | Service Principal App ID | GitHub Actions |
| `databricks-sp-client-secret` | Service Principal Secret | GitHub Actions |

**Databricks Secret Scope (KV-backed):**
```bash
databricks secrets create-scope \
  --scope migration-kv-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id <key-vault-resource-id> \
  --dns-name https://migration-kv.vault.azure.net/
```

**Usage in notebook:**
```python
sql_conn = dbutils.secrets.get(scope="migration-kv-scope", key="sql-connection-string")
adls_key  = dbutils.secrets.get(scope="migration-kv-scope", key="adls-account-key")
```

---

### 2.7 Databricks Asset Bundles (DAB)

**`bundle.yml` structure:**
```yaml
bundle:
  name: migration-dbx-project

workspace:
  host: https://<your-databricks-host>.azuredatabricks.net

targets:
  dev:
    mode: development
    workspace:
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle

resources:
  jobs:
    job_bronze_to_silver:
      name: "[DEV] Bronze to Silver"
      tasks:
        - task_key: bronze_silver
          notebook_task:
            notebook_path: ./notebooks/01_bronze_to_silver.py
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: Standard_DS3_v2
            num_workers: 0
            spark_conf:
              spark.databricks.cluster.profile: singleNode
              spark.master: "local[*]"
            autotermination_minutes: 10

    job_silver_to_gold:
      name: "[DEV] Silver to Gold"
      tasks:
        - task_key: silver_gold
          notebook_task:
            notebook_path: ./notebooks/02_silver_to_gold.py
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: Standard_DS3_v2
            num_workers: 0
            spark_conf:
              spark.databricks.cluster.profile: singleNode
              spark.master: "local[*]"
            autotermination_minutes: 10
```

---

### 2.8 GitHub Actions Workflow

```yaml
# .github/workflows/deploy-databricks.yml
name: Deploy Databricks Bundles

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main

      - name: Deploy bundle to dev
        working-directory: ./databricks
        run: databricks bundle deploy --target dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
          ARM_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}
```

---

## 3. Data Flow — End to End

### Day 0: Initial Full Load
```
1. Developer runs ADF pipeline manually (full load mode)
2. ADF: old_watermark = '1900-01-01' → extracts ALL rows from each table
3. Files land: migration-raw/<table>/ingestion_date=2026-05-10/*.parquet
4. ADF SP: updates watermark to MAX(watermark_col) per table
5. Developer triggers Databricks job_bronze_to_silver manually
6. Notebook reads Bronze partition → applies DQ → MERGEs into Silver Delta
7. Developer triggers Databricks job_silver_to_gold manually
8. Notebook reads Silver → builds Gold star schema + aggregations
9. Verify: row counts, quarantine contents, Gold tables queryable in UC Explorer
```

### Day 1+: Incremental Run (Scheduled)
```
1. ADF trigger fires 02:00 UTC
2. ADF: old_watermark = yesterday's MAX timestamp
3. ADF: new_watermark = current MAX(watermark_col) from source
4. ADF: extracts only rows WHERE wm_col > old_wm AND wm_col <= new_wm
5. New Parquet partition: ingestion_date=today
6. ADF SP: updates watermark
7. Databricks jobs run (can be chained or scheduled separately)
8. MERGE ensures no duplicates in Silver even if same rows re-arrive
```

---

## 4. GitHub Repository Structure

```
migration-dbx-project/
│
├── README.md
│
├── sql/
│   ├── 01_ddl_source_tables.sql       ← CREATE TABLE for all 9 Olist tables
│   ├── 02_watermark_setup.sql         ← watermark_control table + stored proc
│   └── 03_inject_test_data.sql        ← scripts to inject bad records (PROB-03 to 06)
│
├── adf/
│   └── pipelines/
│       └── pl_incremental_ingest.json ← ADF pipeline ARM/JSON export
│
├── databricks/
│   ├── bundle.yml                     ← DAB configuration
│   ├── notebooks/
│   │   ├── 00_setup_unity_catalog.py  ← one-time UC setup: schemas, grants
│   │   ├── 01_bronze_to_silver.py     ← main processing notebook
│   │   └── 02_silver_to_gold.py       ← gold layer notebook
│   └── tests/
│       └── test_dq_rules.py           ← unit tests for DQ functions
│
└── .github/
    └── workflows/
        └── deploy-databricks.yml
```

---

## 5. Monitoring Strategy

| Layer | What to Monitor | Tool | Alert |
|-------|----------------|------|-------|
| ADF | Pipeline success/failure | ADF Monitor → Azure Monitor | Email on failure |
| ADF | Copy activity rows written = 0 (missed extraction) | ADF Monitor metric | Email alert |
| Databricks | Job run success/failure | Databricks Job → Email notifications | Email on failure |
| Silver | Quarantine row count per run | Notebook logs (print statements) | Manual check initially |
| ADLS | Storage size growth anomaly | Azure Monitor metric | Alert if > 2x expected |
| Cost | Azure subscription spend | Cost Management + Budgets | Alert at $10 spend |

> **Practical tip:** Set a $10 Budget Alert in Azure Cost Management on Day 0. Prevents bill shock.

---

## 6. Cost Estimation

| Resource | Tier / Config | Estimated Daily Cost |
|----------|--------------|---------------------|
| Azure SQL PaaS | Basic, 5 DTU | ~$0.15 |
| ADLS Gen2 | LRS, ~1 GB data | ~$0.02 |
| Azure Data Factory | ~20 activity runs/day | ~$0.10 |
| Azure Databricks | Standard, DS3_v2, ~2 hrs/day | ~$1.50 |
| Azure Key Vault | < 10,000 operations | ~$0.01 |
| GitHub Actions | Free tier (2,000 min/month) | $0.00 |
| **Total per day** | | **~$1.80** |
| **Weekend (2 days intensive)** | | **~$4 – $8** |
| **Full project (5–7 days)** | | **~$10 – $15** |

---

## 7. SQL Server On-Premise Equivalent (Bonus Section Preview)

> Full guide provided at end of project. Key differences:

| Aspect | Azure SQL PaaS | SQL Server On-Prem (Simulated) |
|--------|---------------|-------------------------------|
| ADF Connectivity | Azure SQL Linked Service (direct) | Self-Hosted Integration Runtime (SHIR) required |
| SHIR Setup | Not needed | Install SHIR on Windows machine, register with ADF |
| Firewall | Azure SQL firewall rules | Windows Firewall + SQL Server port 1433 |
| Auth | SQL Auth or MSI | SQL Auth or Windows Auth |
| Connection String | `Server=<server>.database.windows.net` | `Server=localhost,1433` (via SHIR) |
| Cost | Included in PaaS | Free (local machine) |
| Network | Public endpoint or Private Endpoint | SHIR tunnels outbound to ADF — no inbound ports needed |

---

*End of Architecture Document. Version 1.0.*
