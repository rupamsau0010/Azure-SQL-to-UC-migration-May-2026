"""
File: load_olist_to_sql.py
Project: MIGRATION-DBX-001
Description:
    Loads all 9 Olist CSVs into Azure SQL.
    Handles encoding issues, date parsing, type coercion, and
    injects simulated watermark columns (created_at) for dimension tables.

Usage:
    pip install pandas pyodbc sqlalchemy python-dotenv
    python load_olist_to_sql.py

Requirements:
    - ODBC Driver 18 for SQL Server installed on Windows
      Download: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
    - Azure SQL firewall allows your current IP (done in Phase 0 step 0.5)
"""

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import urllib
import os
import sys
import logging
from datetime import datetime, timedelta
import random
import numpy as np
import warnings

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# =============================================================================
# CONFIG — update these values
# =============================================================================
SQL_SERVER   = "migration-sql-uc.database.windows.net"
SQL_DATABASE = "migration-db"
SQL_USER     = "sqladmin"
SQL_PASSWORD = "REMOVED"

DATA_DIR = r"D:\Formal\Data Engineering\SQL-to-UC-migration-May-2026\Data\olist"   # path where CSVs are unzipped

# =============================================================================
# SQL Alchemy engine
# =============================================================================
def get_engine():
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True   # critical for bulk insert performance
    )
    return engine


# =============================================================================
# Helper: generate realistic created_at timestamps
# Simulates data arriving over time — this is the watermark column
# for dimension tables that have no native timestamp
# =============================================================================
def generate_created_at(n_rows: int, start="2016-09-01", end="2018-08-31") -> pd.Series:
    """
    Generate n_rows of random DATETIME2 timestamps in [start, end].
    Spread is weighted toward later dates (simulates growing business).
    """
    start_dt = pd.Timestamp(start)
    end_dt   = pd.Timestamp(end)
    total_seconds = int((end_dt - start_dt).total_seconds())

    # Skew toward later dates using quadratic distribution
    rng = np.random.default_rng(seed=42)
    raw = rng.power(1.5, n_rows)  # values in [0,1], weighted toward 1
    offsets = (raw * total_seconds).astype(int)
    timestamps = [start_dt + timedelta(seconds=int(o)) for o in offsets]
    return pd.Series(timestamps)


# =============================================================================
# Loader functions per table
# Each handles that table's specific quirks
# =============================================================================

def load_customers(engine):
    log.info("Loading olist_customers_dataset...")
    df = pd.read_csv(
        os.path.join(DATA_DIR, "olist_customers_dataset.csv"),
        dtype=str,          # read all as string first — avoid pandas guessing
        encoding="utf-8",
        keep_default_na=False,
        na_values=["", "NA", "NULL", "null", "nan", "NaN"]
    )
    log.info(f"  Raw rows: {len(df):,}")

    # Clean
    df["customer_id"]             = df["customer_id"].str.strip()
    df["customer_unique_id"]      = df["customer_unique_id"].str.strip()
    df["customer_zip_code_prefix"] = df["customer_zip_code_prefix"].str.strip().str.zfill(5)
    df["customer_city"]           = df["customer_city"].str.strip().str.lower()
    df["customer_state"]          = df["customer_state"].str.strip().str.upper()

    # Inject simulated watermark
    df["created_at"] = generate_created_at(len(df))

    log.info(f"  NULLs in customer_zip_code_prefix: {df['customer_zip_code_prefix'].isna().sum()}")
    log.info(f"  Duplicate customer_id: {df['customer_id'].duplicated().sum()}")

    df.to_sql("olist_customers_dataset", engine, schema="dbo", if_exists="append", index=False, chunksize=5000)
    log.info(f"  ✓ Loaded {len(df):,} rows")
    return len(df)


def load_geolocation(engine):
    log.info("Loading olist_geolocation_dataset...")
    df = pd.read_csv(
        os.path.join(DATA_DIR, "olist_geolocation_dataset.csv"),
        dtype={
            "geolocation_zip_code_prefix": str,
            "geolocation_city": str,
            "geolocation_state": str
        },
        encoding="utf-8",
        keep_default_na=False,
        na_values=["", "NA", "NULL", "null", "nan", "NaN"]
    )
    log.info(f"  Raw rows: {len(df):,}")

    df["geolocation_zip_code_prefix"] = df["geolocation_zip_code_prefix"].str.strip().str.zfill(5)
    df["geolocation_city"]  = df["geolocation_city"].str.strip().str.lower()
    df["geolocation_state"] = df["geolocation_state"].str.strip().str.upper()
    df["geolocation_lat"]   = pd.to_numeric(df["geolocation_lat"], errors="coerce")
    df["geolocation_lng"]   = pd.to_numeric(df["geolocation_lng"], errors="coerce")

    # --------------------------------------------------------------------------
    # PROB-05: Inject 200 rows with lat/lng swapped
    # This simulates a real data quality issue — coordinates entered reversed
    # You will catch this in Silver via DQ-13 and DQ-14 rules
    # --------------------------------------------------------------------------
    log.info("  Injecting PROB-05: swapping lat/lng on 200 random rows...")
    rng = np.random.default_rng(seed=99)
    bad_indices = rng.choice(len(df), size=200, replace=False)
    df.loc[bad_indices, ["geolocation_lat", "geolocation_lng"]] = \
        df.loc[bad_indices, ["geolocation_lng", "geolocation_lat"]].values
    log.info(f"  PROB-05 rows: lat>90 count = {(df['geolocation_lat'] > 90).sum()}")

    # Inject watermark
    df["created_at"] = generate_created_at(len(df), start="2016-01-01", end="2018-01-01")

    log.info(f"  Total rows (incl. duplicates): {len(df):,}")
    log.info(f"  Unique zip prefixes: {df['geolocation_zip_code_prefix'].nunique():,}")

    # Load in chunks — 1M rows needs chunking
    df.to_sql("olist_geolocation_dataset", engine, schema="dbo", if_exists="append",
              index=False, chunksize=300, method="multi")
    log.info(f"  ✓ Loaded {len(df):,} rows (incl. 200 PROB-05 rows)")
    return len(df)


def load_sellers(engine):
    log.info("Loading olist_sellers_dataset...")
    df = pd.read_csv(
        os.path.join(DATA_DIR, "olist_sellers_dataset.csv"),
        dtype=str,
        encoding="utf-8",
        keep_default_na=False,
        na_values=["", "NA", "NULL", "null", "nan", "NaN"]
    )
    log.info(f"  Raw rows: {len(df):,}")

    df["seller_id"]              = df["seller_id"].str.strip()
    df["seller_zip_code_prefix"] = df["seller_zip_code_prefix"].str.strip().str.zfill(5)
    df["seller_city"]            = df["seller_city"].str.strip().str.lower()
    df["seller_state"]           = df["seller_state"].str.strip().str.upper()
    df["created_at"]             = generate_created_at(len(df))

    df.to_sql("olist_sellers_dataset", engine, schema="dbo", if_exists="append", index=False, chunksize=5000)
    log.info(f"  ✓ Loaded {len(df):,} rows")
    return len(df)


def load_products(engine):
    log.info("Loading olist_products_dataset...")
    df = pd.read_csv(
        os.path.join(DATA_DIR, "olist_products_dataset.csv"),
        dtype=str,
        encoding="utf-8",
        keep_default_na=False,
        na_values=["", "NA", "NULL", "null", "nan", "NaN"]
    )
    log.info(f"  Raw rows: {len(df):,}")

    df["product_id"]            = df["product_id"].str.strip()
    df["product_category_name"] = df["product_category_name"].str.strip().str.lower() \
                                   if "product_category_name" in df.columns else None

    # Cast numeric columns safely
    int_cols = ["product_name_length", "product_description_length",
                "product_photos_qty", "product_weight_g",
                "product_length_cm", "product_height_cm", "product_width_cm"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        null_count = df[col].isna().sum()
        if null_count > 0:
            log.info(f"  NULLs in {col}: {null_count:,}")

    df["created_at"] = generate_created_at(len(df))

    log.info(f"  NULL product_category_name: {df['product_category_name'].isna().sum()}")
    df.to_sql("olist_products_dataset", engine, schema="dbo", if_exists="append", index=False, chunksize=5000)
    log.info(f"  ✓ Loaded {len(df):,} rows")
    return len(df)


def load_category_translation(engine):
    log.info("Loading product_category_name_translation...")
    df = pd.read_csv(
        os.path.join(DATA_DIR, "product_category_name_translation.csv"),
        dtype=str,
        encoding="utf-8",
        keep_default_na=False,
        na_values=["", "NA", "NULL"]
    )
    log.info(f"  Raw rows: {len(df):,}")

    df["product_category_name"]         = df["product_category_name"].str.strip().str.lower()
    df["product_category_name_english"] = df["product_category_name_english"].str.strip().str.lower()

    df.to_sql("product_category_name_translation", engine, schema="dbo", if_exists="append", index=False)
    log.info(f"  ✓ Loaded {len(df):,} rows")
    return len(df)


def load_orders(engine):
    log.info("Loading olist_orders_dataset...")
    df = pd.read_csv(
        os.path.join(DATA_DIR, "olist_orders_dataset.csv"),
        dtype=str,
        encoding="utf-8",
        keep_default_na=False,
        na_values=["", "NA", "NULL", "null", "nan", "NaN"]
    )
    log.info(f"  Raw rows: {len(df):,}")

    df["order_id"]     = df["order_id"].str.strip()
    df["customer_id"]  = df["customer_id"].str.strip()
    df["order_status"] = df["order_status"].str.strip().str.lower()

    # Parse datetime columns — Olist uses format: 2017-09-13 08:59:02
    ts_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in ts_cols:
        df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M:%S", errors="coerce")
        null_count = df[col].isna().sum()
        if null_count > 0:
            log.info(f"  NULLs in {col}: {null_count:,}")

    log.info(f"  Order status distribution:\n{df['order_status'].value_counts().to_string()}")
    log.info(f"  Date range: {df['order_purchase_timestamp'].min()} → {df['order_purchase_timestamp'].max()}")

    df.to_sql("olist_orders_dataset", engine, schema="dbo", if_exists="append", index=False, chunksize=5000)
    log.info(f"  ✓ Loaded {len(df):,} rows")
    return len(df)


def load_order_items(engine):
    log.info("Loading olist_order_items_dataset...")
    df = pd.read_csv(
        os.path.join(DATA_DIR, "olist_order_items_dataset.csv"),
        dtype=str,
        encoding="utf-8",
        keep_default_na=False,
        na_values=["", "NA", "NULL", "null", "nan", "NaN"]
    )
    log.info(f"  Raw rows: {len(df):,}")

    df["order_id"]    = df["order_id"].str.strip()
    df["product_id"]  = df["product_id"].str.strip()
    df["seller_id"]   = df["seller_id"].str.strip()
    df["order_item_id"] = pd.to_numeric(df["order_item_id"], errors="coerce")
    df["price"]         = pd.to_numeric(df["price"], errors="coerce")
    df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce")
    df["shipping_limit_date"] = pd.to_datetime(
        df["shipping_limit_date"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
    )

    log.info(f"  Orders with multiple items: {df[df['order_item_id'] > 1]['order_id'].nunique():,}")
    log.info(f"  Max items per order: {df['order_item_id'].max()}")
    log.info(f"  NULLs in price: {df['price'].isna().sum()}")

    df.to_sql("olist_order_items_dataset", engine, schema="dbo", if_exists="append", index=False, chunksize=5000)
    log.info(f"  ✓ Loaded {len(df):,} rows")
    return len(df)


def load_order_payments(engine):
    log.info("Loading olist_order_payments_dataset...")
    df = pd.read_csv(
        os.path.join(DATA_DIR, "olist_order_payments_dataset.csv"),
        dtype=str,
        encoding="utf-8",
        keep_default_na=False,
        na_values=["", "NA", "NULL", "null", "nan", "NaN"]
    )
    log.info(f"  Raw rows: {len(df):,}")

    df["order_id"]             = df["order_id"].str.strip()
    df["payment_type"]         = df["payment_type"].str.strip().str.lower()
    df["payment_sequential"]   = pd.to_numeric(df["payment_sequential"], errors="coerce")
    df["payment_installments"] = pd.to_numeric(df["payment_installments"], errors="coerce")
    df["payment_value"]        = pd.to_numeric(df["payment_value"], errors="coerce")

    log.info(f"  Payment types:\n{df['payment_type'].value_counts().to_string()}")
    log.info(f"  Orders with multiple payments: {df[df['payment_sequential'] > 1]['order_id'].nunique():,}")

    df.to_sql("olist_order_payments_dataset", engine, schema="dbo", if_exists="append", index=False, chunksize=5000)
    log.info(f"  ✓ Loaded {len(df):,} rows")
    return len(df)


def load_order_reviews(engine):
    log.info("Loading olist_order_reviews_dataset...")
    df = pd.read_csv(
        os.path.join(DATA_DIR, "olist_order_reviews_dataset.csv"),
        dtype=str,
        encoding="latin-1",   # ← IMPORTANT: reviews have Latin-1 encoding (Portuguese chars)
        keep_default_na=False,
        na_values=["", "NA", "NULL", "null", "nan", "NaN"]
    )
    log.info(f"  Raw rows: {len(df):,}")

    # Re-encode to utf-8 compatible strings
    for col in ["review_comment_title", "review_comment_message"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: x.encode("latin-1").decode("utf-8", errors="replace")
                if isinstance(x, str) else x
            )

    df["review_id"]    = df["review_id"].str.strip()
    df["order_id"]     = df["order_id"].str.strip()
    df["review_score"] = pd.to_numeric(df["review_score"], errors="coerce")

    ts_cols = ["review_creation_date", "review_answer_timestamp"]
    for col in ts_cols:
        df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M:%S", errors="coerce")

    log.info(f"  NULL review_comment_message: {df['review_comment_message'].isna().sum():,} ({df['review_comment_message'].isna().mean()*100:.1f}%)")
    log.info(f"  review_score distribution:\n{df['review_score'].value_counts().sort_index().to_string()}")

    # --------------------------------------------------------------------------
    # PROB-03: Inject 500 rows with NULL review_id
    # ADF will land these in Bronze. Silver DQ-01-equivalent check on review_id
    # will quarantine them — gives you a chance to see quarantine in action.
    # --------------------------------------------------------------------------
    log.info("  Injecting PROB-03: setting review_id to NULL for 500 rows...")
    rng = np.random.default_rng(seed=77)
    bad_indices = rng.choice(len(df), size=500, replace=False)
    df.loc[bad_indices, "review_id"] = None
    log.info(f"  PROB-03 injected: {df['review_id'].isna().sum()} NULL review_ids")

    df.to_sql("olist_order_reviews_dataset", engine, schema="dbo", if_exists="append", index=False, chunksize=5000)
    log.info(f"  ✓ Loaded {len(df):,} rows (incl. 500 PROB-03 rows)")
    return len(df)


# =============================================================================
# MAIN
# =============================================================================
def main():
    log.info("=" * 60)
    log.info("MIGRATION-DBX-001 — Phase 1: Source Data Load")
    log.info("=" * 60)

    # Validate data directory
    if not os.path.exists(DATA_DIR):
        log.error(f"Data directory not found: {DATA_DIR}")
        log.error("Run Step 1.1 first to download and unzip Olist dataset.")
        sys.exit(1)

    # Test connection
    log.info("Testing SQL connection...")
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT @@VERSION"))
            log.info(f"Connected: {result.fetchone()[0][:50]}")
    except Exception as e:
        log.error(f"Connection failed: {e}")
        log.error("Check firewall rules (Phase 0 Step 0.5) and credentials.")
        sys.exit(1)

    # Load order:
    # Dimension-like tables first (no FK constraints in SQL, but logical order)
    # Then transactional tables
    results = {}
    loaders = [
        ("customers",            load_customers),
        ("sellers",              load_sellers),
        ("products",             load_products),
        ("category_translation", load_category_translation),
        ("geolocation",          load_geolocation),    # largest — load last of dims
        ("orders",               load_orders),
        ("order_items",          load_order_items),
        ("order_payments",       load_order_payments),
        ("order_reviews",        load_order_reviews),  # PROB-03 injected here
    ]

    for name, fn in loaders:
        try:
            count = fn(engine)
            results[name] = count
        except Exception as e:
            log.error(f"FAILED loading {name}: {e}")
            results[name] = -1

    # Summary
    log.info("\n" + "=" * 60)
    log.info("LOAD SUMMARY")
    log.info("=" * 60)

    expected = {
        "customers":            99_441,
        "sellers":               3_095,
        "products":             32_951,
        "category_translation":     71,
        "geolocation":       1_000_163,
        "orders":               99_441,
        "order_items":         112_650,
        "order_payments":      103_886,
        "order_reviews":        99_224,
    }

    total_loaded = 0
    all_ok = True
    for name, count in results.items():
        exp = expected.get(name, "?")
        status = "✓" if count > 0 else "✗"
        delta = f"(expected ~{exp:,})" if isinstance(exp, int) else ""
        log.info(f"  {status} {name:<25} {count:>10,} rows  {delta}")
        if count > 0:
            total_loaded += count
        else:
            all_ok = False

    log.info(f"\n  TOTAL: {total_loaded:,} rows")
    log.info(f"  STATUS: {'ALL GOOD ✓' if all_ok else 'SOME FAILURES ✗ — check logs above'}")

    # Injected problems reminder
    log.info("\n" + "=" * 60)
    log.info("INJECTED PROBLEMS (you will face these in later phases):")
    log.info("  PROB-03: 500 NULL review_id rows in olist_order_reviews_dataset")
    log.info("  PROB-05: 200 lat/lng swapped rows in olist_geolocation_dataset")
    log.info("  More problems injected by architect during Silver/Gold phases.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
