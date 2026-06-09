"""
run_gold_pipeline.py
---------------------
Run this file to build all Gold layer aggregation tables.

HOW TO RUN:
    python3 run_gold_pipeline.py

WHAT IT PRODUCES:
    gold_daily_revenue        → revenue per day (powers trend chart)
    gold_revenue_by_category  → revenue per product category (powers bar chart)
    gold_revenue_by_state     → revenue per state (powers Brazil map)
    gold_customer_satisfaction→ review scores by category (powers scorecard)
    gold_seller_performance   → revenue and ratings per seller

IMPORTANT:
    Run run_silver_pipeline.py first (Silver must exist before Gold)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from gold.gold_runner import GoldRunner


def main():
    print("=" * 60)
    print("  EXECUTIVE DASHBOARD GUARDIAN")
    print("  Gold Layer — Business Aggregations")
    print("=" * 60)

    print("\n[1/3] Starting Spark session...")
    builder = (
        SparkSession.builder
        .appName("executive_dashboard_guardian_gold")
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

    print("\n[2/3] Building Gold aggregation tables...")
    print("      (joining Silver tables and calculating KPIs)\n")

    runner = GoldRunner(spark)
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
        print("\n  All Gold tables created successfully!")
        print("\n  What each table powers in Power BI:")
        print("  gold_daily_revenue        → Revenue trend line chart")
        print("  gold_revenue_by_category  → Category bar chart")
        print("  gold_revenue_by_state     → Brazil revenue map")
        print("  gold_customer_satisfaction→ Satisfaction scorecard")
        print("  gold_seller_performance   → Seller leaderboard")
        print("\n  Data is ready at: data/delta/gold/")
    else:
        print("\n  Some aggregations failed. Check logs above.")

    print("\n" + "=" * 60)
    print("  Next: Connect to Power BI for executive dashboard")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
