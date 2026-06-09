# Databricks notebook source
# Title: 01_bronze_ingestion
# Phase: 1 — Bronze Layer Data Ingestion
# Project: Executive Dashboard Guardian
# ─────────────────────────────────────────────────────────────────────────────
# HOW TO USE THIS NOTEBOOK IN DATABRICKS COMMUNITY EDITION:
# 1. Import this file into Databricks as a notebook (.py with # COMMAND ----------)
# 2. Attach to a cluster running DBR 13.x+ (includes Delta Lake and PySpark)
# 3. Upload your Kaggle CSVs to DBFS: /FileStore/executive_dashboard_guardian/raw/
# 4. Run all cells top-to-bottom
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# MAGIC %md
# MAGIC # Executive Dashboard Guardian
# MAGIC ## Phase 1: Bronze Layer Ingestion
# MAGIC
# MAGIC This notebook ingests the raw Olist e-commerce CSV files into Bronze Delta tables.
# MAGIC Every load is audited. Schema drift is detected. Row counts are validated.
# MAGIC No data reaches the dashboard without passing through this layer.

# COMMAND ----------

# Install loguru if not available (Databricks doesn't include it by default)
# In a real cluster, you'd add this to the cluster init script or libraries tab
%pip install loguru python-dotenv --quiet

# COMMAND ----------

# ─── CONFIGURATION ──────────────────────────────────────────────────────────
# In Databricks Community Edition, files uploaded to DBFS are at /dbfs/FileStore/
# Adjust these paths based on where you uploaded your Kaggle CSVs.

RAW_DATA_PATH = "/dbfs/FileStore/executive_dashboard_guardian/raw"
DELTA_BASE_PATH = "/dbfs/FileStore/executive_dashboard_guardian/delta"

# Override the path config for Databricks environment
import os
os.environ["RAW_DATA_PATH"] = RAW_DATA_PATH
os.environ["DELTA_BASE_PATH"] = DELTA_BASE_PATH
os.environ["PIPELINE_ENV"] = "databricks_community"

print(f"Raw data path:  {RAW_DATA_PATH}")
print(f"Delta base path: {DELTA_BASE_PATH}")

# COMMAND ----------

# ─── VERIFY SOURCE FILES EXIST ──────────────────────────────────────────────
import os

expected_files = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
]

print("Checking source files...")
all_present = True
for f in expected_files:
    path = os.path.join(RAW_DATA_PATH, f)
    exists = os.path.exists(path)
    status = "✓ Found" if exists else "✗ MISSING"
    print(f"  {status}: {f}")
    if not exists:
        all_present = False

if not all_present:
    raise FileNotFoundError(
        "Some source files are missing! "
        "Upload your Kaggle CSVs to DBFS before running this notebook."
    )
print("\nAll source files present. Proceeding with ingestion.")

# COMMAND ----------

# ─── SPARK SESSION ──────────────────────────────────────────────────────────
# In Databricks, `spark` is already available as a global variable.
# This check lets us also run locally with spark-submit.

try:
    spark  # Already exists in Databricks
    print(f"Using existing Spark session: {spark.version}")
except NameError:
    # Local execution
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("executive_dashboard_guardian_bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    print(f"Created local Spark session: {spark.version}")

# COMMAND ----------

# ─── RUN BRONZE INGESTION PIPELINE ──────────────────────────────────────────
# We import from our project modules.
# In Databricks, add the project root to sys.path or use %run to import.

import sys
sys.path.insert(0, "/dbfs/FileStore/executive_dashboard_guardian/src")

from ingestion.bronze_loader import BronzeLoader

loader = BronzeLoader(spark, write_mode="overwrite")
results = loader.run_full_pipeline()

# COMMAND ----------

# ─── DISPLAY AUDIT LOG ──────────────────────────────────────────────────────
from config.settings import path_cfg, table_cfg

audit_path = f"{path_cfg.audit_path}/{table_cfg.audit_log}"
audit_df = spark.read.format("delta").load(audit_path)

print("=== INGESTION AUDIT LOG ===")
display(
    audit_df.select(
        "table_name",
        "load_status",
        "rows_loaded",
        "rows_expected_min",
        "schema_drift_detected",
        "error_message",
        "ingestion_timestamp",
    ).orderBy("ingestion_timestamp")
)

# COMMAND ----------

# ─── VALIDATE BRONZE TABLES ─────────────────────────────────────────────────
# Quick sanity check: read each Bronze table and show shape + sample

bronze_tables = {
    "bronze_orders": f"{path_cfg.bronze_path}/bronze_orders",
    "bronze_order_items": f"{path_cfg.bronze_path}/bronze_order_items",
    "bronze_order_payments": f"{path_cfg.bronze_path}/bronze_order_payments",
    "bronze_customers": f"{path_cfg.bronze_path}/bronze_customers",
    "bronze_products": f"{path_cfg.bronze_path}/bronze_products",
}

for table_name, delta_path in bronze_tables.items():
    df = spark.read.format("delta").load(delta_path)
    print(f"\n{'='*50}")
    print(f"TABLE: {table_name}")
    print(f"  Rows: {df.count():,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Metadata columns: {[c for c in df.columns if c.startswith('_')]}")
    df.show(3, truncate=True)

# COMMAND ----------

# ─── DELTA LAKE TIME TRAVEL DEMO ────────────────────────────────────────────
# One of Delta's killer features: query any previous version of your data

orders_path = f"{path_cfg.bronze_path}/bronze_orders"

# Show table history (all writes ever made to this Delta table)
print("=== Delta Table History for bronze_orders ===")
display(spark.sql(f"DESCRIBE HISTORY delta.`{orders_path}`"))

# You can query a previous version like this:
# df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(orders_path)

# COMMAND ----------

# ─── SQL INTERFACE (Register as temp views) ─────────────────────────────────
# Register Delta tables as SQL views so we can query them with plain SQL

for table_name, delta_path in bronze_tables.items():
    spark.read.format("delta").load(delta_path).createOrReplaceTempView(table_name)

# Now you can run SQL queries directly
result = spark.sql("""
    SELECT
        order_status,
        COUNT(*) as order_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
    FROM bronze_orders
    GROUP BY order_status
    ORDER BY order_count DESC
""")
display(result)

# COMMAND ----------

# ─── FINAL SUMMARY ──────────────────────────────────────────────────────────
print("\n" + "="*60)
print("PHASE 1 COMPLETE: BRONZE LAYER INGESTED")
print("="*60)
print("\nWhat was built:")
print("  ✓ 5 Bronze Delta tables with raw Olist data")
print("  ✓ Metadata columns: _ingestion_timestamp, _source_file, _row_hash")
print("  ✓ Schema drift detection on all tables")
print("  ✓ Row count validation against expected minimums")
print("  ✓ Full audit log of every load attempt")
print("  ✓ Delta Lake time travel enabled")
print("\nNext: Phase 2 — Data Quality Checks (missing txns, duplicates, nulls)")
