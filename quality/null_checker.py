"""
quality/null_checker.py
------------------------
Rule 2: Detect null values in critical columns.

WHY THIS MATTERS:
Some columns MUST never be null for the dashboard to work correctly.
If order_id is null → we can't track the order
If payment_value is null → revenue calculation is wrong
If customer_id is null → we can't attribute revenue to a customer

NULL CATEGORIES WE CHECK:
  CRITICAL  → column is a primary key or used in revenue calculation
              Pipeline should STOP if these are null
  WARNING   → column is important but dashboard can partially work
              Alert the team but continue
"""

from typing import List, Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from loguru import logger

from quality.base_rule import BaseRule, Violation
from config.settings import CRITICAL_NOT_NULL_COLUMNS


class NullChecker(BaseRule):
    """
    Checks every critical column for null values.
    Uses the CRITICAL_NOT_NULL_COLUMNS config from settings.py.
    """

    def _check(self, df: DataFrame, table_name: str) -> List[Violation]:
        violations = []

        critical_cols = CRITICAL_NOT_NULL_COLUMNS.get(table_name, [])
        if not critical_cols:
            logger.info(f"[NullChecker] No critical columns defined for {table_name} — skipping")
            return []

        total_rows = df.count()
        if total_rows == 0:
            return [self._violation(
                table_name=table_name,
                severity="CRITICAL",
                description="Table has ZERO rows — complete data loss detected.",
                affected_rows=0,
            )]

        for col_name in critical_cols:
            # Skip if column doesn't exist (schema drift — caught by Phase 1)
            if col_name not in df.columns:
                logger.warning(f"[NullChecker] Column '{col_name}' not found in {table_name}")
                continue

            null_count = df.filter(F.col(col_name).isNull()).count()

            if null_count == 0:
                logger.info(f"[NullChecker] {table_name}.{col_name} → no nulls ✓")
                continue

            null_pct = round((null_count / total_rows) * 100, 2)

            # Severity based on null percentage
            if null_pct >= 10:
                severity = "CRITICAL"
            elif null_pct >= 1:
                severity = "WARNING"
            else:
                severity = "INFO"

            # Get sample non-null values around the nulls for context
            sample_rows = (
                df.filter(F.col(col_name).isNull())
                .limit(3)
                .select([c for c in df.columns if c != col_name][:3])
                .collect()
            )
            sample = str([dict(zip(row.__fields__, row)) for row in sample_rows])

            violations.append(self._violation(
                table_name=table_name,
                severity=severity,
                description=(
                    f"Column '{col_name}' has {null_count:,} null values "
                    f"({null_pct}% of {total_rows:,} total rows). "
                    f"This column is marked as critical and must never be null."
                ),
                affected_rows=null_count,
                sample_values=sample,
            ))

        return violations
