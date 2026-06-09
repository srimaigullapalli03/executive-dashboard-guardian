"""
quality/staleness_detector.py
------------------------------
Rule 3: Detect stale data feeds.

WHAT IS STALE DATA?
Stale data = data that hasn't been updated in longer than expected.

Example: Your pipeline runs every day at 2am.
By 9am the executive opens the dashboard.
If the pipeline failed silently, the dashboard shows yesterday's data
but LOOKS like today's data. Nobody knows.

HOW WE DETECT IT:
We check the _ingestion_timestamp column we added in Phase 1.
If the most recent record is older than our threshold → STALE.

THRESHOLDS (how old is too old):
  - Daily pipeline   → alert if data is older than 25 hours
  - Hourly pipeline  → alert if data is older than 90 minutes
  For this project we use 25 hours (daily pipeline assumption)
"""

from datetime import datetime, timezone, timedelta
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from loguru import logger

from quality.base_rule import BaseRule, Violation

# How old can data be before we flag it as stale?
# 25 hours = gives 1 hour buffer for a daily pipeline
STALENESS_THRESHOLD_HOURS = 25


class StalenessDetector(BaseRule):
    """
    Checks if data in a Bronze table is fresh enough.
    Uses _ingestion_timestamp added by Phase 1 pipeline.
    """

    def _check(self, df: DataFrame, table_name: str) -> List[Violation]:

        if "_ingestion_timestamp" not in df.columns:
            logger.warning(f"[StalenessDetector] No _ingestion_timestamp in {table_name} — skipping")
            return []

        # Find the most recent ingestion timestamp in this table
        latest_row = (
            df.agg(F.max("_ingestion_timestamp").alias("latest_ts"))
            .collect()[0]
        )
        latest_ts = latest_row["latest_ts"]

        if latest_ts is None:
            return [self._violation(
                table_name=table_name,
                severity="CRITICAL",
                description="Cannot determine data freshness — _ingestion_timestamp is null for all rows.",
                affected_rows=0,
            )]

        # Calculate how old the data is
        now = datetime.now(timezone.utc)

        # Handle both timezone-aware and timezone-naive timestamps
        if latest_ts.tzinfo is None:
            latest_ts = latest_ts.replace(tzinfo=timezone.utc)

        age_hours = (now - latest_ts).total_seconds() / 3600
        age_str = f"{age_hours:.1f} hours"

        logger.info(f"[StalenessDetector] {table_name} → latest data is {age_str} old")

        if age_hours <= STALENESS_THRESHOLD_HOURS:
            return []  # Data is fresh — no violation

        # Data is stale — determine severity
        if age_hours > 72:
            severity = "CRITICAL"
            description = (
                f"Data is {age_str} old — SEVERELY STALE. "
                f"Pipeline may not have run for {int(age_hours/24)} days. "
                f"Dashboard is showing dangerously outdated information."
            )
        elif age_hours > 48:
            severity = "CRITICAL"
            description = (
                f"Data is {age_str} old — pipeline missed at least 1 full day. "
                f"Executives are making decisions on 2-day-old data."
            )
        else:
            severity = "WARNING"
            description = (
                f"Data is {age_str} old — exceeds the {STALENESS_THRESHOLD_HOURS}h freshness threshold. "
                f"Expected daily pipeline may have been delayed."
            )

        return [self._violation(
            table_name=table_name,
            severity=severity,
            description=description,
            affected_rows=df.count(),
            sample_values=f"Latest ingestion timestamp: {latest_ts.isoformat()}",
        )]
