"""
quality/revenue_anomaly_detector.py
-------------------------------------
Rule 4: Detect abnormal revenue values using statistical analysis.

WHAT IS A REVENUE ANOMALY?
A value that is so far from normal that it's likely a data error.

REAL EXAMPLES:
  - A pricing bug charges $99,999 instead of $99.99
  - A test transaction of $0.01 slips into production data
  - A currency conversion bug multiplies all prices by 100
  - A NULL replaced by 0 makes average revenue look artificially low

HOW WE DETECT IT — Z-SCORE METHOD:
Z-score measures how many standard deviations a value is from the mean.

  Z-score = (value - mean) / standard_deviation

  Z = 0   → exactly average
  Z = 1   → 1 standard deviation above average (normal)
  Z = 2   → 2 standard deviations above (slightly unusual)
  Z = 3   → 3 standard deviations above (rare, flag it)
  Z > 3   → almost certainly an error in real business data

Example with payment_value:
  Mean payment = $120
  Std dev      = $80
  A payment of $9,000 has Z-score = (9000 - 120) / 80 = 111 → ANOMALY

WE ALSO CHECK:
  - Zero-value payments (could be test data or bugs)
  - Negative payments (refunds are OK, but flag them)
  - Extreme outliers using IQR (Interquartile Range) as a second method
"""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from loguru import logger

from quality.base_rule import BaseRule, Violation

# Columns to run revenue anomaly detection on, per table
REVENUE_COLUMNS = {
    "bronze_order_payments": ["payment_value"],
    "bronze_order_items":    ["price", "freight_value"],
}

# Z-score threshold — values beyond this are flagged
ZSCORE_THRESHOLD = 3.0

# IQR multiplier — values beyond Q3 + (multiplier * IQR) are flagged
IQR_MULTIPLIER = 3.0


class RevenueAnomalyDetector(BaseRule):
    """
    Detects statistically abnormal revenue values using Z-score and IQR methods.
    Only runs on tables that have revenue columns defined above.
    """

    def _check(self, df: DataFrame, table_name: str) -> List[Violation]:
        revenue_cols = REVENUE_COLUMNS.get(table_name)
        if not revenue_cols:
            logger.info(f"[RevenueAnomalyDetector] No revenue columns for {table_name} — skipping")
            return []

        violations = []
        for col_name in revenue_cols:
            if col_name not in df.columns:
                continue
            violations += self._check_column(df, table_name, col_name)

        return violations

    def _check_column(
        self, df: DataFrame, table_name: str, col_name: str
    ) -> List[Violation]:
        violations = []

        # Filter to only non-null numeric rows for stats
        numeric_df = df.filter(F.col(col_name).isNotNull())
        total_rows = numeric_df.count()

        if total_rows < 30:
            logger.info(f"[RevenueAnomalyDetector] Not enough rows for stats on {col_name}")
            return []

        # ── STEP 1: Calculate statistics ──────────────────────────
        stats = numeric_df.agg(
            F.mean(col_name).alias("mean"),
            F.stddev(col_name).alias("stddev"),
            F.percentile_approx(col_name, 0.25).alias("q1"),
            F.percentile_approx(col_name, 0.75).alias("q3"),
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val"),
            F.count(col_name).alias("count"),
        ).collect()[0]

        mean = stats["mean"]
        stddev = stats["stddev"] or 0
        q1 = stats["q1"]
        q3 = stats["q3"]
        iqr = q3 - q1

        logger.info(
            f"[RevenueAnomalyDetector] {table_name}.{col_name} stats → "
            f"mean={mean:.2f}, stddev={stddev:.2f}, "
            f"min={stats['min_val']:.2f}, max={stats['max_val']:.2f}"
        )

        # ── STEP 2: Z-score outliers ─────────────────────────────
        if stddev > 0:
            zscore_outliers = numeric_df.filter(
                F.abs((F.col(col_name) - mean) / stddev) > ZSCORE_THRESHOLD
            )
            zscore_count = zscore_outliers.count()

            if zscore_count > 0:
                sample_vals = [
                    round(row[col_name], 2)
                    for row in zscore_outliers.select(col_name).limit(5).collect()
                ]
                violations.append(self._violation(
                    table_name=table_name,
                    severity="WARNING",
                    description=(
                        f"Column '{col_name}' has {zscore_count:,} values with "
                        f"Z-score > {ZSCORE_THRESHOLD} (mean={mean:.2f}, stddev={stddev:.2f}). "
                        f"These values are statistically abnormal and may indicate "
                        f"pricing errors, test data, or system bugs."
                    ),
                    affected_rows=zscore_count,
                    sample_values=f"Outlier values: {sample_vals}",
                ))

        # ── STEP 3: IQR outliers ──────────────────────────────────
        if iqr > 0:
            upper_fence = q3 + (IQR_MULTIPLIER * iqr)
            lower_fence = q1 - (IQR_MULTIPLIER * iqr)

            iqr_outliers = numeric_df.filter(
                (F.col(col_name) > upper_fence) | (F.col(col_name) < lower_fence)
            )
            iqr_count = iqr_outliers.count()

            if iqr_count > 0:
                sample_vals = [
                    round(row[col_name], 2)
                    for row in iqr_outliers.select(col_name).limit(5).collect()
                ]
                violations.append(self._violation(
                    table_name=table_name,
                    severity="WARNING",
                    description=(
                        f"Column '{col_name}' has {iqr_count:,} IQR outliers "
                        f"(outside [{lower_fence:.2f}, {upper_fence:.2f}]). "
                        f"Q1={q1:.2f}, Q3={q3:.2f}, IQR={iqr:.2f}."
                    ),
                    affected_rows=iqr_count,
                    sample_values=f"Outlier values: {sample_vals}",
                ))

        # ── STEP 4: Zero-value payments ───────────────────────────
        zero_count = numeric_df.filter(F.col(col_name) == 0).count()
        if zero_count > 0:
            violations.append(self._violation(
                table_name=table_name,
                severity="INFO",
                description=(
                    f"Column '{col_name}' has {zero_count:,} zero values. "
                    f"These may be test transactions, free orders, or data errors."
                ),
                affected_rows=zero_count,
                sample_values=f"Zero-value count: {zero_count}",
            ))

        # ── STEP 5: Negative values ───────────────────────────────
        negative_count = numeric_df.filter(F.col(col_name) < 0).count()
        if negative_count > 0:
            violations.append(self._violation(
                table_name=table_name,
                severity="WARNING",
                description=(
                    f"Column '{col_name}' has {negative_count:,} negative values. "
                    f"Negative payments may indicate refunds — verify if expected."
                ),
                affected_rows=negative_count,
                sample_values=f"Negative value count: {negative_count}",
            ))

        return violations
