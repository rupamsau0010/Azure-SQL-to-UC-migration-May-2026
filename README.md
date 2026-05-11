This project migrates Olist e-commerce transactional data from Azure SQL PaaS into a Databricks Lakehouse governed by Unity Catalog. Azure Data Factory orchestrates watermark-based incremental extraction into ADLS Gen2 (Bronze layer). Databricks jobs process data through Silver (DQ-validated Delta) and Gold (aggregated, star-schema Delta) layers. Full CI/CD is implemented via GitHub Actions and Databricks Asset Bundles. The project targets cost efficiency (< $15 total) while covering every real-world migration concern: incremental loading, data quality, secret management, idempotency, monitoring, and automated deployment.

For more details: visit - Docs/PRD.md or Docs/STTM.md

-- ignore