"""
quality/duplicate_check.py
---------------------------
Detects duplicate records in Bronze tables using the _row_hash column
we added during Phase 1 ingestion.

BUSINESS PROBLEM:
If order_001 gets loaded twice, revenue dashboards count it twice.
A $50 order becomes $100 in the dashboard.
An executive sees inflated revenue and makes wrong decisions.

HOW IT WORKS:
Every row has a _row_hash (MD5 fingerprint of all its columns).
If two rows have the SAME hash → they are 100% identical → DUPLICATE.

We group by _row_hash, count occurrences, and flag any count > 1.
"""

from pyspark.sql import functions as F
from loguru import logger

from quality.base_check import BaseCheck, CheckResult, Violation


class DuplicateCheck(BaseCheck):
    """
    Scans a Bronze table for exact duplicate records using _row_hash.
    """

    def run(self, table_name: str) -> CheckResult:
        import time
        start = time.time()
        logger.info(f"[DuplicateCheck] Starting on {table_name}")

        try:
            df = self.read_bronze_table(table_name)
            total_rows = df.count()

            # ── Find duplicates ───────────────────────────────────
            # Group by _row_hash, count how many rows share each hash
            # If count > 1, those rows are duplicates
            duplicate_df = (
                df.groupBy("_row_hash")
                .agg(F.count("*").alias("occurrence_count"))
                .filter(F.col("occurrence_count") > 1)
            )

            duplicate_count = duplicate_df.count()
            duration = round(time.time() - start, 2)

            if duplicate_count == 0:
                logger.success(f"[DuplicateCheck] {table_name} — PASSED. No duplicates found.")
                return CheckResult(
                    check_name="duplicate_check",
                    table_name=table_name,
                    status="PASSED",
                    rows_scanned=total_rows,
                    duration_seconds=duration,
                )

            # ── Duplicates found — build violation ────────────────
            # Get sample of the actual duplicate rows for investigation
            duplicate_hashes = duplicate_df.select("_row_hash")
            sample_dupes = df.join(duplicate_hashes, on="_row_hash", how="inner")
            sample_json = self.records_to_json(sample_dupes)

            # Total extra rows = sum of (occurrence_count - 1) for all duplicate groups
            extra_rows_df = duplicate_df.agg(
                F.sum(F.col("occurrence_count") - 1).alias("extra_rows")
            ).collect()
            extra_rows = int(extra_rows_df[0]["extra_rows"] or 0)

            severity = "CRITICAL" if extra_rows > 100 else "WARNING"

            violation = Violation(
                check_name="duplicate_check",
                table_name=table_name,
                severity=severity,
                violation_count=extra_rows,
                violation_detail=(
                    f"{duplicate_count} duplicate hash groups found in {table_name}. "
                    f"{extra_rows} extra rows that should not exist. "
                    f"These inflate aggregations like revenue and order counts."
                ),
                sample_records=sample_json,
            )

            logger.warning(
                f"[DuplicateCheck] {table_name} — {severity}. "
                f"{extra_rows} duplicate rows found across {duplicate_count} hash groups."
            )

            return CheckResult(
                check_name="duplicate_check",
                table_name=table_name,
                status="FAILED",
                violations=[violation],
                rows_scanned=total_rows,
                duration_seconds=duration,
            )

        except Exception as e:
            logger.error(f"[DuplicateCheck] {table_name} — ERROR: {e}")
            return CheckResult(
                check_name="duplicate_check",
                table_name=table_name,
                status="ERROR",
                error_message=str(e),
            )
