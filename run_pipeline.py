"""
run_pipeline.py
----------------
Run this file to execute Phase 1 Bronze ingestion.

HOW TO RUN:
    python run_pipeline.py

REQUIREMENTS:
    1. Put your 5 Kaggle CSV files inside data/raw/ folder first
    2. Install dependencies: pip install -r requirements.txt
"""

import sys
import os

# Make sure Python can find our config/ and ingestion/ folders
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from ingestion.bronze_loader import BronzeLoader

def main():
    print("=" * 60)
    print("  EXECUTIVE DASHBOARD GUARDIAN")
    print("  Phase 1: Bronze Layer Ingestion")
    print("=" * 60)

    # ── Create Spark Session with Delta Lake properly configured ─
    print("\n[1/3] Starting Spark session...")

    builder = (
        SparkSession.builder
        .appName("executive_dashboard_guardian")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
    )

    # This is the key fix — it downloads and wires in the Delta JAR automatically
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"    Spark {spark.version} ready ✓")
    print(f"    Delta Lake configured ✓")

    # ── Run Bronze Pipeline ──────────────────────────────────────
    print("\n[2/3] Running Bronze ingestion pipeline...")
    loader = BronzeLoader(spark, write_mode="overwrite")
    results = loader.run_full_pipeline()

    # ── Print Results ────────────────────────────────────────────
    print("\n[3/3] Pipeline complete. Results:")
    print("-" * 40)
    all_good = True
    for table, status in results.items():
        icon = "✓" if status == "SUCCESS" else "⚠" if status == "PARTIAL" else "✗"
        print(f"  {icon}  {table:<40} {status}")
        if status != "SUCCESS":
            all_good = False

    print("-" * 40)
    if all_good:
        print("\n  All tables loaded successfully!")
        print("  Check data/delta/bronze/ to see your Delta tables.")
        print("  Check data/delta/audit/  to see the audit log.")
    else:
        print("\n  Some tables had issues. Check logs/ folder for details.")

    print("\nNext step: Phase 2 — Data Quality Rules Engine")
    spark.stop()


if __name__ == "__main__":
    main()
