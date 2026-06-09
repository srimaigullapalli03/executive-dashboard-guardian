"""
quality/volume_check.py
------------------------
Detects when significantly fewer records than expected arrived in a table.

BUSINESS PROBLEM:
Your pipeline runs every night and normally loads ~300 new orders.
One night it only loads 12. Nobody notices. The dashboard shows
nearly zero new revenue for the day — but it's not real, the
pipeline just partially failed.

HOW IT WORKS:
We compare today's total row count against the expected minimum
threshold defined in config. If the count is below threshold,
we flag it as a volume drop.

For a more advanced version (which we note for interviewers),
you'd compare against a rolling 7-day average instead of a fixed number.
"""

from pyspark.sql import functions as F
from loguru import logger

from quality.base_check import BaseCheck, CheckResult, Violation
from config.settings import row_thresholds, table_cfg


# Map table names to their minimum thresholds
VOLUME_THRESHOLDS = {
    table_cfg.orders:      row_thresholds.orders,
    table_cfg.order_items: row_thresholds.order_items,
    table_cfg.payments:    row_thresholds.payments,
    table_cfg.customers:   row_thresholds.customers,
    table_cfg.products:    row_thresholds.products,
    table_cfg.sellers:     row_thresholds.sellers,
    table_cfg.reviews:     row_thresholds.reviews,
    table_cfg.geolocation: row_thresholds.geolocation,
}


class VolumeCheck(BaseCheck):
    """
    Checks whether a Bronze table has the expected minimum number of rows.
    """

    def run(self, table_name: str) -> CheckResult:
        import time
        start = time.time()
        logger.info(f"[VolumeCheck] Starting on {table_name}")

        try:
            min_expected = VOLUME_THRESHOLDS.get(table_name, 0)

            if min_expected == 0:
                logger.info(f"[VolumeCheck] No threshold set for {table_name} — skipping.")
                return CheckResult(
                    check_name="volume_check",
                    table_name=table_name,
                    status="PASSED",
                    rows_scanned=0,
                )

            df = self.read_bronze_table(table_name)
            actual_count = df.count()
            duration = round(time.time() - start, 2)

            pct_of_expected = round((actual_count / min_expected) * 100, 1)

            logger.info(
                f"[VolumeCheck] {table_name} — "
                f"Actual: {actual_count:,} | Expected min: {min_expected:,} | "
                f"({pct_of_expected}% of expected)"
            )

            if actual_count >= min_expected:
                logger.success(
                    f"[VolumeCheck] {table_name} — PASSED. "
                    f"{actual_count:,} rows (min expected: {min_expected:,})."
                )
                return CheckResult(
                    check_name="volume_check",
                    table_name=table_name,
                    status="PASSED",
                    rows_scanned=actual_count,
                    duration_seconds=duration,
                )

            # ── Volume is below threshold ─────────────────────────
            shortfall = min_expected - actual_count
            shortfall_pct = round((shortfall / min_expected) * 100, 1)
            severity = "CRITICAL" if shortfall_pct > 20 else "WARNING"

            violation = Violation(
                check_name="volume_check",
                table_name=table_name,
                severity=severity,
                violation_count=shortfall,
                violation_detail=(
                    f"{table_name} has only {actual_count:,} rows. "
                    f"Expected at least {min_expected:,}. "
                    f"Missing {shortfall:,} rows ({shortfall_pct}% below threshold). "
                    f"Possible causes: pipeline partial failure, source system outage, "
                    f"or data was not delivered."
                ),
                sample_records="[]",
            )

            logger.warning(
                f"[VolumeCheck] {table_name} — {severity}. "
                f"Only {actual_count:,} rows, expected {min_expected:,}."
            )

            return CheckResult(
                check_name="volume_check",
                table_name=table_name,
                status="FAILED",
                violations=[violation],
                rows_scanned=actual_count,
                duration_seconds=duration,
            )

        except Exception as e:
            logger.error(f"[VolumeCheck] {table_name} — ERROR: {e}")
            return CheckResult(
                check_name="volume_check",
                table_name=table_name,
                status="ERROR",
                error_message=str(e),
            )
