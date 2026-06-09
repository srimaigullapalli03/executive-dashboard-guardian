"""
run_quality_checks.py
----------------------
Run this file to execute all 5 data quality checks.

HOW TO RUN:
    python3 run_quality_checks.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from quality.dq_runner import DQRunner


def main():
    print("=" * 60)
    print("  EXECUTIVE DASHBOARD GUARDIAN")
    print("  Data Quality Rules Engine")
    print("=" * 60)

    print("\n[1/3] Starting Spark session...")
    builder = (
        SparkSession.builder
        .appName("executive_dashboard_guardian_quality")
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

    print("\n[2/3] Running data quality checks...")
    print("      (checking for duplicates, nulls, staleness,")
    print("       volume drops, and revenue anomalies)\n")

    runner = DQRunner(spark)
    summary = runner.run_all_checks()

    print("\n[3/3] Results:")
    print(f"\n  Run ID: {summary['run_id']}")
    print("-" * 60)

    check_groups = {}
    for result in summary["results"]:
        key = result.check_name
        if key not in check_groups:
            check_groups[key] = []
        check_groups[key].append(result)

    for check_name, results in check_groups.items():
        statuses = [r.status for r in results]
        if all(s == "PASSED" for s in statuses):
            overall = "PASSED"
            icon = "✓"
        elif any(s == "ERROR" for s in statuses):
            overall = "ERROR"
            icon = "✗"
        elif any(s == "FAILED" for s in statuses):
            overall = "FAILED"
            icon = "✗"
        else:
            overall = "WARNING"
            icon = "⚠"

        total_violations = sum(len(r.violations) for r in results)
        violation_str = f"({total_violations} violation(s))" if total_violations > 0 else ""
        print(f"  {icon}  {check_name:<30} {overall:<10} {violation_str}")

    print("-" * 60)
    print(f"\n  Total checks run : {summary['total']}")
    print(f"  Passed           : {summary['passed']}")
    print(f"  Failed           : {summary['failed']}")
    print(f"  Errors           : {summary['errors']}")
    print(f"\n  Overall Status   : {summary['overall']}")

    if summary["overall"] == "PASSED":
        print("\n  All quality checks passed!")
        print("  Data is safe to use in executive dashboards.")
    else:
        print("\n  Some checks failed. Review violations before")
        print("  allowing data to reach executive dashboards.")
        print("  View violations: data/delta/quality/dq_violations")

    print("\n" + "=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
