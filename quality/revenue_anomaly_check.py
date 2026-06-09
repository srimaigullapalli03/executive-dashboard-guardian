"""
quality/revenue_anomaly_check.py
---------------------------------
Detects abnormal revenue values using statistical analysis (Z-score).

BUSINESS PROBLEM:
Today's revenue looks like $2,100.
Last 30 days averaged $45,000 per day.
Is this a real slow day? Or did a join fail and drop 95% of records?
Without this check, nobody knows until an executive asks.

HOW IT WORKS (Z-score explained simply):
1. Calculate total revenue per day over all historical data
2. Find the average daily revenue and how much it normally varies
3. Z-score = how many "standard deviations" today is from the average
   - Z-score of 0   = exactly average (normal)
   - Z-score of 1   = slightly above/below (normal)
   - Z-score of 2   = noticeably different (watch it)
   - Z-score of 3+  = extremely unusual → FLAG IT

Example:
   Average daily revenue = $45,000
   Normal variation      = ±$5,000
   Today's revenue       = $2,100
   Z-score               = ($2,100 - $45,000) / $5,000 = -8.6
   → WAY outside normal → CRITICAL alert
"""

import math
from pyspark.sql import functions as F
from loguru import logger

from quality.base_check import BaseCheck, CheckResult, Violation
from config.settings import dq_cfg


class RevenueAnomalyCheck(BaseCheck):
    """
    Detects statistically abnormal revenue days using Z-score analysis.
    Runs on bronze_order_payments joined with bronze_orders.
    """

    def run(self, table_name: str = "bronze_order_payments") -> CheckResult:
        import time
        start = time.time()
        logger.info(f"[RevenueAnomalyCheck] Starting revenue analysis")

        try:
            # ── Load payments and orders tables ───────────────────
            payments_path = f"{self.bronze_base_path}/bronze_order_payments"
            orders_path   = f"{self.bronze_base_path}/bronze_orders"

            payments_df = self.spark.read.format("delta").load(payments_path)
            orders_df   = self.spark.read.format("delta").load(orders_path)

            # ── Join to get daily revenue ─────────────────────────
            # We need order_purchase_timestamp from orders
            # and payment_value from payments
            joined_df = payments_df.join(
                orders_df.select("order_id", "order_purchase_timestamp"),
                on="order_id",
                how="inner"
            )

            # Extract date from timestamp string
            daily_revenue_df = (
                joined_df
                .withColumn(
                    "order_date",
                    F.to_date(F.col("order_purchase_timestamp"))
                )
                .groupBy("order_date")
                .agg(F.sum("payment_value").alias("daily_revenue"))
                .orderBy("order_date")
            )

            # ── Calculate statistics ──────────────────────────────
            stats = daily_revenue_df.agg(
                F.avg("daily_revenue").alias("mean_revenue"),
                F.stddev("daily_revenue").alias("stddev_revenue"),
                F.count("*").alias("day_count"),
                F.max("daily_revenue").alias("max_revenue"),
                F.min("daily_revenue").alias("min_revenue"),
            ).collect()[0]

            mean_rev   = float(stats["mean_revenue"] or 0)
            stddev_rev = float(stats["stddev_revenue"] or 1)
            day_count  = int(stats["day_count"] or 0)

            if day_count < 7:
                logger.info(
                    f"[RevenueAnomalyCheck] Not enough history ({day_count} days). "
                    f"Need at least 7 days for meaningful anomaly detection."
                )
                return CheckResult(
                    check_name="revenue_anomaly_check",
                    table_name=table_name,
                    status="PASSED",
                    rows_scanned=day_count,
                )

            logger.info(
                f"[RevenueAnomalyCheck] Stats over {day_count} days | "
                f"Mean: ${mean_rev:,.2f} | StdDev: ${stddev_rev:,.2f}"
            )

            # ── Find anomalous days ───────────────────────────────
            threshold = dq_cfg.revenue_zscore_threshold  # default: 3.0

            anomalous_days = (
                daily_revenue_df
                .withColumn(
                    "z_score",
                    F.abs(
                        (F.col("daily_revenue") - F.lit(mean_rev))
                        / F.lit(max(stddev_rev, 0.01))  # avoid division by zero
                    )
                )
                .filter(F.col("z_score") > threshold)
                .orderBy(F.col("z_score").desc())
            )

            anomaly_count = anomalous_days.count()
            duration = round(time.time() - start, 2)

            if anomaly_count == 0:
                logger.success(
                    f"[RevenueAnomalyCheck] PASSED. "
                    f"No anomalous revenue days found across {day_count} days of data."
                )
                return CheckResult(
                    check_name="revenue_anomaly_check",
                    table_name=table_name,
                    status="PASSED",
                    rows_scanned=day_count,
                    duration_seconds=duration,
                )

            # ── Anomalies found ───────────────────────────────────
            sample_json = self.records_to_json(anomalous_days)
            top_anomaly = anomalous_days.first()
            top_date    = top_anomaly["order_date"]
            top_rev     = float(top_anomaly["daily_revenue"])
            top_z       = float(top_anomaly["z_score"])

            severity = "CRITICAL" if top_z > 5 else "WARNING"

            violation = Violation(
                check_name="revenue_anomaly_check",
                table_name=table_name,
                severity=severity,
                violation_count=anomaly_count,
                violation_detail=(
                    f"{anomaly_count} anomalous revenue day(s) detected. "
                    f"Most extreme: {top_date} with revenue ${top_rev:,.2f} "
                    f"(Z-score: {top_z:.1f}, threshold: {threshold}). "
                    f"Historical average: ${mean_rev:,.2f}/day. "
                    f"Investigate whether this reflects real business activity "
                    f"or a data pipeline failure."
                ),
                sample_records=sample_json,
            )

            logger.warning(
                f"[RevenueAnomalyCheck] {severity}. "
                f"{anomaly_count} anomalous days. "
                f"Worst: {top_date} = ${top_rev:,.2f} (Z={top_z:.1f})"
            )

            return CheckResult(
                check_name="revenue_anomaly_check",
                table_name=table_name,
                status="FAILED",
                violations=[violation],
                rows_scanned=day_count,
                duration_seconds=duration,
            )

        except Exception as e:
            logger.error(f"[RevenueAnomalyCheck] ERROR: {e}")
            return CheckResult(
                check_name="revenue_anomaly_check",
                table_name=table_name,
                status="ERROR",
                error_message=str(e),
            )
