"""
quality/staleness_check.py
---------------------------
Detects when data hasn't been refreshed recently enough.

BUSINESS PROBLEM:
A dashboard says "Live Revenue" but the pipeline broke 30 hours ago
and nobody noticed. Executives are looking at yesterday's data
thinking it's today's. They see no new orders and think sales are dead.

HOW IT WORKS:
Every row in Bronze has a _ingestion_timestamp added by our pipeline.
We find the MOST RECENT timestamp in the table.
If that timestamp is older than our threshold (default 24 hours), flag it.

This tells us: "The last time fresh data arrived was X hours ago."
"""

from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F
from loguru import logger

from quality.base_check import BaseCheck, CheckResult, Violation
from config.settings import dq_cfg


class StalenessCheck(BaseCheck):
    """
    Checks how recently data was loaded into a Bronze table.
    Flags tables where data hasn't been refreshed within the threshold.
    """

    def run(self, table_name: str) -> CheckResult:
        import time
        start = time.time()
        logger.info(f"[StalenessCheck] Starting on {table_name}")

        try:
            df = self.read_bronze_table(table_name)
            total_rows = df.count()

            # ── Find the most recent ingestion timestamp ──────────
            latest_ts_row = df.agg(
                F.max("_ingestion_timestamp").alias("latest_ts")
            ).collect()

            if not latest_ts_row or latest_ts_row[0]["latest_ts"] is None:
                violation = Violation(
                    check_name="staleness_check",
                    table_name=table_name,
                    severity="CRITICAL",
                    violation_count=1,
                    violation_detail=(
                        f"No _ingestion_timestamp found in {table_name}. "
                        f"Cannot determine when data was last loaded."
                    ),
                    sample_records="[]",
                )
                return CheckResult(
                    check_name="staleness_check",
                    table_name=table_name,
                    status="FAILED",
                    violations=[violation],
                    rows_scanned=total_rows,
                )

            latest_ts = latest_ts_row[0]["latest_ts"]

            # Make latest_ts timezone-aware for comparison
            if latest_ts.tzinfo is None:
                latest_ts = latest_ts.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)
            age_hours = round((now - latest_ts).total_seconds() / 3600, 1)
            threshold_hours = dq_cfg.staleness_threshold_hours

            duration = round(time.time() - start, 2)

            logger.info(
                f"[StalenessCheck] {table_name} — "
                f"Last loaded: {latest_ts} ({age_hours}h ago) | "
                f"Threshold: {threshold_hours}h"
            )

            if age_hours <= threshold_hours:
                logger.success(
                    f"[StalenessCheck] {table_name} — PASSED. "
                    f"Data is {age_hours}h old (within {threshold_hours}h threshold)."
                )
                return CheckResult(
                    check_name="staleness_check",
                    table_name=table_name,
                    status="PASSED",
                    rows_scanned=total_rows,
                    duration_seconds=duration,
                )

            # ── Data is stale ─────────────────────────────────────
            severity = "CRITICAL" if age_hours > (threshold_hours * 2) else "WARNING"

            violation = Violation(
                check_name="staleness_check",
                table_name=table_name,
                severity=severity,
                violation_count=1,
                violation_detail=(
                    f"Data in {table_name} is {age_hours} hours old. "
                    f"Last ingestion: {latest_ts}. "
                    f"Threshold is {threshold_hours} hours. "
                    f"Executives may be viewing outdated information."
                ),
                sample_records="[]",
            )

            logger.warning(
                f"[StalenessCheck] {table_name} — {severity}. "
                f"Data is {age_hours}h old (threshold: {threshold_hours}h)."
            )

            return CheckResult(
                check_name="staleness_check",
                table_name=table_name,
                status="FAILED",
                violations=[violation],
                rows_scanned=total_rows,
                duration_seconds=duration,
            )

        except Exception as e:
            logger.error(f"[StalenessCheck] {table_name} — ERROR: {e}")
            return CheckResult(
                check_name="staleness_check",
                table_name=table_name,
                status="ERROR",
                error_message=str(e),
            )
