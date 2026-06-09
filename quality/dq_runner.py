"""
quality/dq_runner.py
---------------------
Orchestrates all 5 data quality checks and writes results to Delta tables.

TWO OUTPUT TABLES:
1. dq_violations  — every individual problem found (one row per violation)
2. dq_summary     — one row per pipeline run (overall PASSED/FAILED status)

This is the file that ties everything together.
Think of it as the quality control manager who assigns work to 5 inspectors,
collects their reports, and writes the final quality report.
"""

import uuid
from datetime import datetime, timezone
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, TimestampType, LongType
)
from loguru import logger

from config.settings import path_cfg, pipeline_cfg, SOURCE_FILE_MAP
from quality.base_check import CheckResult, Violation
from quality.duplicate_check import DuplicateCheck
from quality.null_check import NullCheck
from quality.staleness_check import StalenessCheck
from quality.volume_check import VolumeCheck
from quality.revenue_anomaly_check import RevenueAnomalyCheck


# ── Output schemas ────────────────────────────────────────────────────────────

VIOLATIONS_SCHEMA = StructType([
    StructField("run_id",            StringType(),   False),
    StructField("check_name",        StringType(),   True),
    StructField("table_name",        StringType(),   True),
    StructField("severity",          StringType(),   True),
    StructField("violation_count",   LongType(),     True),
    StructField("violation_detail",  StringType(),   True),
    StructField("sample_records",    StringType(),   True),
    StructField("check_timestamp",   TimestampType(),True),
    StructField("environment",       StringType(),   True),
])

SUMMARY_SCHEMA = StructType([
    StructField("run_id",            StringType(),   False),
    StructField("pipeline_name",     StringType(),   True),
    StructField("total_checks",      LongType(),     True),
    StructField("passed",            LongType(),     True),
    StructField("failed",            LongType(),     True),
    StructField("warnings",          LongType(),     True),
    StructField("overall_status",    StringType(),   True),
    StructField("run_timestamp",     TimestampType(),True),
    StructField("environment",       StringType(),   True),
])


class DQRunner:
    """
    Runs all data quality checks and writes results to Delta.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.run_id = str(uuid.uuid4())[:8]   # Short ID for this run
        self.bronze_path = f"{path_cfg.bronze_path}"
        self.quality_path = f"{path_cfg.delta_base_path}/quality"
        self.violations_path = f"{self.quality_path}/dq_violations"
        self.summary_path = f"{self.quality_path}/dq_summary"
        self.run_timestamp = datetime.now(timezone.utc)

        # Instantiate all 5 checks
        self.checks = {
            "duplicate_check":       DuplicateCheck(spark, self.bronze_path),
            "null_check":            NullCheck(spark, self.bronze_path),
            "staleness_check":       StalenessCheck(spark, self.bronze_path),
            "volume_check":          VolumeCheck(spark, self.bronze_path),
            "revenue_anomaly_check": RevenueAnomalyCheck(spark, self.bronze_path),
        }

    def run_all_checks(self) -> dict:
        """
        Run all 5 checks across all relevant tables.
        Returns summary dict for printing.
        """
        logger.info("=" * 60)
        logger.info(f"DATA QUALITY ENGINE — STARTING | run_id={self.run_id}")
        logger.info("=" * 60)

        all_results: List[CheckResult] = []

        # Tables to run each check on
        core_tables = [
            "bronze_orders",
            "bronze_order_items",
            "bronze_order_payments",
            "bronze_customers",
            "bronze_products",
        ]

        # ── Run Duplicate Check on all core tables ────────────────
        for table in core_tables:
            result = self.checks["duplicate_check"].run(table)
            all_results.append(result)

        # ── Run Null Check on all core tables ─────────────────────
        for table in core_tables:
            result = self.checks["null_check"].run(table)
            all_results.append(result)

        # ── Run Staleness Check on all core tables ────────────────
        for table in core_tables:
            result = self.checks["staleness_check"].run(table)
            all_results.append(result)

        # ── Run Volume Check on all tables ────────────────────────
        for table in core_tables:
            result = self.checks["volume_check"].run(table)
            all_results.append(result)

        # ── Run Revenue Anomaly Check (cross-table) ───────────────
        result = self.checks["revenue_anomaly_check"].run("bronze_order_payments")
        all_results.append(result)

        # ── Write results to Delta ────────────────────────────────
        self._write_violations(all_results)
        summary = self._write_summary(all_results)

        return summary

    def _write_violations(self, results: List[CheckResult]) -> None:
        """Collect all violations from all results and write to Delta."""
        all_violations = []

        for result in results:
            for v in result.violations:
                all_violations.append((
                    self.run_id,
                    v.check_name,
                    v.table_name,
                    v.severity,
                    int(v.violation_count),
                    v.violation_detail,
                    v.sample_records,
                    v.check_timestamp,
                    pipeline_cfg.env,
                ))

        if not all_violations:
            logger.info("No violations to write — all checks passed!")
            return

        violations_df = self.spark.createDataFrame(all_violations, schema=VIOLATIONS_SCHEMA)
        (
            violations_df.write
            .format("delta")
            .mode("append")
            .save(self.violations_path)
        )
        logger.info(f"Wrote {len(all_violations)} violations to {self.violations_path}")

    def _write_summary(self, results: List[CheckResult]) -> dict:
        """Write one summary row for this entire run."""
        passed  = sum(1 for r in results if r.status == "PASSED")
        failed  = sum(1 for r in results if r.status == "FAILED")
        errors  = sum(1 for r in results if r.status == "ERROR")
        total   = len(results)
        overall = "PASSED" if failed == 0 and errors == 0 else "FAILED"

        summary_row = [(
            self.run_id,
            pipeline_cfg.pipeline_name,
            total,
            passed,
            failed,
            errors,
            overall,
            self.run_timestamp,
            pipeline_cfg.env,
        )]

        summary_df = self.spark.createDataFrame(summary_row, schema=SUMMARY_SCHEMA)
        (
            summary_df.write
            .format("delta")
            .mode("append")
            .save(self.summary_path)
        )

        return {
            "run_id": self.run_id,
            "total": total,
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "overall": overall,
            "results": results,
        }
