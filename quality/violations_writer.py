"""
quality/violations_writer.py
-----------------------------
Writes all data quality violations to a Delta table.

WHY A SEPARATE VIOLATIONS TABLE?
In production, data quality results need to be:
  1. Queryable by SQL ("show me all CRITICAL violations this week")
  2. Connected to Power BI (executives see a data health dashboard)
  3. Used by alerting systems (send Slack/email when CRITICAL issues found)
  4. Auditable (track whether issues were fixed over time)

A Delta table gives us all of this automatically.
"""

import os
from typing import List
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)
from loguru import logger

from quality.base_rule import Violation
from config.settings import path_cfg


VIOLATIONS_SCHEMA = StructType([
    StructField("violation_id",    StringType(),   nullable=False),
    StructField("rule_name",       StringType(),   nullable=False),
    StructField("table_name",      StringType(),   nullable=False),
    StructField("severity",        StringType(),   nullable=False),
    StructField("description",     StringType(),   nullable=True),
    StructField("affected_rows",   IntegerType(),  nullable=True),
    StructField("sample_values",   StringType(),   nullable=True),
    StructField("check_timestamp", TimestampType(),nullable=False),
    StructField("pipeline_run_id", StringType(),   nullable=True),
])


class ViolationsWriter:
    """Saves violation records to the dq_violations Delta table."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.violations_path = os.path.join(
            path_cfg.delta_base_path, "quality", "dq_violations"
        )

    def write(self, violations: List[Violation]) -> int:
        """
        Write a list of violations to Delta.
        Returns the count of violations written.
        """
        if not violations:
            logger.info("[ViolationsWriter] No violations to write.")
            return 0

        records = [
            (
                v.violation_id,
                v.rule_name,
                v.table_name,
                v.severity,
                v.description,
                v.affected_rows,
                v.sample_values,
                v.check_timestamp,
                v.pipeline_run_id,
            )
            for v in violations
        ]

        df = self.spark.createDataFrame(records, schema=VIOLATIONS_SCHEMA)

        os.makedirs(self.violations_path, exist_ok=True)
        (
            df.write
            .format("delta")
            .mode("append")
            .save(self.violations_path)
        )

        logger.info(f"[ViolationsWriter] Wrote {len(violations)} violation(s) to Delta.")
        return len(violations)

    def print_summary(self, violations: List[Violation]) -> None:
        """Print a clean summary to the console."""
        if not violations:
            print("\n  ✅ All data quality checks PASSED — no violations found!")
            return

        critical = [v for v in violations if v.severity == "CRITICAL"]
        warnings  = [v for v in violations if v.severity == "WARNING"]
        info      = [v for v in violations if v.severity == "INFO"]

        print(f"\n{'='*60}")
        print(f"  DATA QUALITY REPORT")
        print(f"{'='*60}")
        print(f"  🔴 CRITICAL : {len(critical)}")
        print(f"  🟡 WARNING  : {len(warnings)}")
        print(f"  🔵 INFO     : {len(info)}")
        print(f"{'='*60}")

        for v in violations:
            icon = "🔴" if v.severity == "CRITICAL" else "🟡" if v.severity == "WARNING" else "🔵"
            print(f"\n  {icon} [{v.severity}] {v.rule_name} → {v.table_name}")
            print(f"     {v.description}")
            print(f"     Affected rows: {v.affected_rows:,}")
            if v.sample_values:
                print(f"     Sample: {v.sample_values[:100]}")

        print(f"\n{'='*60}")
        if critical:
            print("  ⛔ ACTION REQUIRED: CRITICAL violations must be resolved")
            print("     before data reaches the executive dashboard.")
        print(f"{'='*60}\n")
