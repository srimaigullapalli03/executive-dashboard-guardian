"""
gold/gold_daily_revenue.py
---------------------------
Creates the gold_daily_revenue table.

WHAT THIS TABLE ANSWERS:
"How much revenue did we make each day?"

This is the #1 KPI every executive wants to see.
It powers the revenue trend line chart in Power BI.

HOW IT WORKS:
Join silver_orders + silver_order_payments
Group by order_date
Calculate total revenue, order count, average order value

COLUMNS PRODUCED:
- order_date          → the day
- total_revenue       → sum of all payments that day
- order_count         → number of orders that day
- avg_order_value     → average payment per order
- total_freight       → total shipping cost that day
- order_year          → for easy filtering in Power BI
- order_month         → for easy filtering in Power BI
- order_quarter       → for easy filtering in Power BI
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger


class GoldDailyRevenue:
    """Builds gold_daily_revenue aggregation table."""

    def __init__(self, spark: SparkSession, silver_path: str, gold_path: str):
        self.spark = spark
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.table_name = "gold_daily_revenue"

    def run(self) -> int:
        logger.info(f"[{self.table_name}] Starting Gold aggregation")

        # ── Read Silver tables ────────────────────────────────────
        orders_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_orders")
            .select(
                "order_id",
                "order_date",
                "order_year",
                "order_month",
                "order_quarter",
                "order_status",
                "delivery_days",
                "delivered_on_time",
            )
        )

        payments_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_order_payments")
            .select("order_id", "payment_value")
        )

        items_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_order_items")
            .select("order_id", "freight_value")
        )

        # ── Join orders + payments ────────────────────────────────
        revenue_df = orders_df.join(payments_df, on="order_id", how="left")

        # ── Join with items to get freight ────────────────────────
        freight_df = (
            items_df
            .groupBy("order_id")
            .agg(F.sum("freight_value").alias("total_freight_per_order"))
        )
        revenue_df = revenue_df.join(freight_df, on="order_id", how="left")

        # ── Only include delivered orders for revenue ─────────────
        # Pending/cancelled orders shouldn't count as revenue yet
        revenue_df = revenue_df.filter(
            F.col("order_status").isin(["delivered", "shipped", "invoiced"])
        )

        # ── Aggregate by day ──────────────────────────────────────
        gold_df = (
            revenue_df
            .groupBy(
                "order_date",
                "order_year",
                "order_month",
                "order_quarter",
            )
            .agg(
                F.round(F.sum("payment_value"), 2).alias("total_revenue"),
                F.countDistinct("order_id").alias("order_count"),
                F.round(F.avg("payment_value"), 2).alias("avg_order_value"),
                F.round(F.sum("total_freight_per_order"), 2).alias("total_freight"),
                F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
                F.sum(
                    F.when(F.col("delivered_on_time") == True, 1).otherwise(0)
                ).alias("on_time_deliveries"),
            )
            .orderBy("order_date")
        )

        # ── Add revenue vs previous day comparison ────────────────
        from pyspark.sql.window import Window
        window = Window.orderBy("order_date")

        gold_df = (
            gold_df
            .withColumn(
                "prev_day_revenue",
                F.lag("total_revenue", 1).over(window)
            )
            .withColumn(
                "revenue_change_pct",
                F.round(
                    ((F.col("total_revenue") - F.col("prev_day_revenue"))
                     / F.col("prev_day_revenue")) * 100, 2
                )
            )
        )

        # ── Write Gold table ──────────────────────────────────────
        output_path = f"{self.gold_path}/{self.table_name}"
        (
            gold_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(output_path)
        )

        row_count = gold_df.count()
        logger.success(
            f"[{self.table_name}] Complete. {row_count:,} rows written. "
            f"(one row per day)"
        )
        return row_count
