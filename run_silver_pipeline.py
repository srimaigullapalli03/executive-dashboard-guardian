"""
run_silver_pipeline.py
-----------------------
Run this file to execute the Silver layer transformation.

HOW TO RUN:
    python3 run_silver_pipeline.py

WHAT IT DOES:
    Reads all 5 Bronze Delta tables
    Cleans and transforms the data
    Writes clean Silver Delta tables to data/delta/silver/

IMPORTANT:
    Run run_pipeline.py first (Bronze must exist before Silver)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from silver.silver_runner import SilverRunner


def main():
    print("=" * 60)
    print("  EXECUTIVE DASHBOARD GUARDIAN")
    print("  Silver Layer — Data Cleaning & Transformation")
    print("=" * 60)

    print("\n[1/3] Starting Spark session...")
    builder = (
        SparkSession.builder
        .appName("executive_dashboard_guardian_silver")
        .master("local[*]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("    Spark ready ✓")

    print("\n[2/3] Running Silver transformations...")
    print("      (casting timestamps, standardizing text,")
    print("       adding derived columns, filtering nulls)\n")

    runner = SilverRunner(spark)
    results = runner.run_all()

    print("\n[3/3] Results:")
    print("-" * 60)
    all_good = True
    for table, result in results.items():
        icon = "✓" if result["status"] == "SUCCESS" else "✗"
        rows = f"{result['rows']:,} rows" if result["status"] == "SUCCESS" else result.get("error", "")
        print(f"  {icon}  {table:<35} {result['status']:<10} {rows}")
        if result["status"] != "SUCCESS":
            all_good = False

    print("-" * 60)

    if all_good:
        print("\n  All Silver tables created successfully!")
        print("  Clean data is ready at: data/delta/silver/")
        print("\n  What changed in Silver vs Bronze:")
        print("  - Timestamps are now proper date types (not text)")
        print("  - Text fields are standardized (lowercase, trimmed)")
        print("  - Derived columns added (delivery_days, order_month, etc)")
        print("  - Null critical rows removed")
        print("  - Metadata columns removed")
    else:
        print("\n  Some transformations failed. Check logs above.")

    print("\n" + "=" * 60)
    print("  Next: Build Gold layer (business aggregations)")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
