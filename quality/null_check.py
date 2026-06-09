"""
quality/null_check.py
----------------------
Detects NULL values in critical columns that should never be empty.

BUSINESS PROBLEM:
If payment_value is NULL for 500 orders, the revenue dashboard
calculates those orders as $0. Total revenue appears lower than reality.
Executives think sales are down and panic — but the data is just broken.

HOW IT WORKS:
For each table we have a list of CRITICAL_NOT_NULL_COLUMNS defined
in config/settings.py. We check every one of those columns for NULLs.
Any NULL found in a critical column = violation.
"""

from typing import List
from pyspark.sql import functions as F
from loguru import logger

from quality.base_check import BaseCheck, CheckResult, Violation
from config.settings import CRITICAL_NOT_NULL_COLUMNS


class NullCheck(BaseCheck):
    """
    Scans critical columns in a Bronze table for NULL values.
    """

    def run(self, table_name: str) -> CheckResult:
        import time
        start = time.time()
        logger.info(f"[NullCheck] Starting on {table_name}")

        try:
            critical_cols = CRITICAL_NOT_NULL_COLUMNS.get(table_name, [])

            if not critical_cols:
                logger.info(f"[NullCheck] No critical columns defined for {table_name} — skipping.")
                return CheckResult(
                    check_name="null_check",
                    table_name=table_name,
                    status="PASSED",
                    rows_scanned=0,
                    duration_seconds=0,
                )

            df = self.read_bronze_table(table_name)
            total_rows = df.count()
            violations = []

            # ── Check each critical column for NULLs ─────────────
            for col_name in critical_cols:

                if col_name not in df.columns:
                    logger.warning(
                        f"[NullCheck] Column '{col_name}' not found in {table_name} — "
                        f"possibly a schema drift issue."
                    )
                    continue

                null_count = df.filter(F.col(col_name).isNull()).count()

                if null_count > 0:
                    # Get sample rows where this column is null
                    sample_df = df.filter(F.col(col_name).isNull())
                    sample_json = self.records_to_json(sample_df)

                    # How bad is it?
                    null_pct = round((null_count / total_rows) * 100, 2)
                    severity = "CRITICAL" if null_pct > 1.0 else "WARNING"

                    violation = Violation(
                        check_name="null_check",
                        table_name=table_name,
                        severity=severity,
                        violation_count=null_count,
                        violation_detail=(
                            f"Column '{col_name}' has {null_count} NULL values "
                            f"({null_pct}% of {total_rows:,} total rows). "
                            f"This is a critical field that must never be empty."
                        ),
                        sample_records=sample_json,
                    )
                    violations.append(violation)
                    logger.warning(
                        f"[NullCheck] {table_name}.{col_name} — "
                        f"{severity}: {null_count} NULLs ({null_pct}%)"
                    )
                else:
                    logger.info(f"[NullCheck] {table_name}.{col_name} — OK (no nulls)")

            duration = round(time.time() - start, 2)

            if not violations:
                logger.success(f"[NullCheck] {table_name} — PASSED. All critical columns are clean.")
                return CheckResult(
                    check_name="null_check",
                    table_name=table_name,
                    status="PASSED",
                    rows_scanned=total_rows,
                    duration_seconds=duration,
                )

            return CheckResult(
                check_name="null_check",
                table_name=table_name,
                status="FAILED",
                violations=violations,
                rows_scanned=total_rows,
                duration_seconds=duration,
            )

        except Exception as e:
            logger.error(f"[NullCheck] {table_name} — ERROR: {e}")
            return CheckResult(
                check_name="null_check",
                table_name=table_name,
                status="ERROR",
                error_message=str(e),
            )
