"""
gold/gold_revenue_by_state.py
------------------------------
Creates the gold_revenue_by_state table.

WHAT THIS TABLE ANSWERS:
"Which states in Brazil generate the most revenue?"

This feeds the Power BI MAP visual.
Each row = one Brazilian state with its revenue and coordinates.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from loguru import logger


class GoldRevenueByState:
    """Builds gold_revenue_by_state aggregation table for Power BI map."""

    def __init__(self, spark: SparkSession, silver_path: str, gold_path: str):
        self.spark = spark
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.table_name = "gold_revenue_by_state"

    def run(self) -> int:
        logger.info(f"[{self.table_name}] Starting Gold aggregation")

        # ── Read Silver tables ────────────────────────────────────
        orders_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_orders")
            .select("order_id", "customer_id", "order_status",
                    "order_year", "order_month")
            .filter(F.col("order_status").isin(
                ["delivered", "shipped", "invoiced"]
            ))
        )

        payments_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_order_payments")
            .select("order_id", "payment_value")
        )

        customers_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_customers")
            .select(
                "customer_id",
                "customer_zip_code_prefix",
                "customer_state",
            )
        )

        geolocation_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_geolocation")
            .select(
                "geolocation_zip_code_prefix",
                "geolocation_lat",
                "geolocation_lng",
            )
        )

        # ── Join step by step ─────────────────────────────────────
        # Step 1: orders + payments
        step1 = orders_df.join(payments_df, on="order_id", how="left")

        # Step 2: + customers
        step2 = step1.join(customers_df, on="customer_id", how="left")

        # Step 3: + geolocation via zip code
        step3 = step2.join(
            geolocation_df,
            step2["customer_zip_code_prefix"] ==
            geolocation_df["geolocation_zip_code_prefix"],
            how="left"
        )

        # ── Aggregate by state ────────────────────────────────────
        gold_df = (
            step3
            .groupBy("customer_state")
            .agg(
                F.round(F.sum("payment_value"), 2).alias("total_revenue"),
                F.countDistinct("order_id").alias("order_count"),
                F.round(F.avg("payment_value"), 2).alias("avg_order_value"),
                F.round(F.avg("geolocation_lat"), 4).alias("state_lat"),
                F.round(F.avg("geolocation_lng"), 4).alias("state_lng"),
            )
            .orderBy(F.col("total_revenue").desc())
        )

        # ── Add revenue rank ──────────────────────────────────────
        window = Window.orderBy(F.col("total_revenue").desc())
        gold_df = gold_df.withColumn(
            "revenue_rank", F.rank().over(window)
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
            f"(one row per Brazilian state)"
        )
        return row_count
