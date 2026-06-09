"""
gold/gold_seller_performance.py
--------------------------------
Creates the gold_seller_performance table.

WHAT THIS TABLE ANSWERS:
"Which sellers are generating the most revenue and have the best ratings?"

Executives use this to identify top performers and underperformers.

HOW IT WORKS:
Join silver_order_items + silver_sellers + silver_order_reviews + silver_orders
Group by seller_id
Calculate revenue, order count, avg review score per seller
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger


class GoldSellerPerformance:
    """Builds gold_seller_performance aggregation table."""

    def __init__(self, spark: SparkSession, silver_path: str, gold_path: str):
        self.spark = spark
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.table_name = "gold_seller_performance"

    def run(self) -> int:
        logger.info(f"[{self.table_name}] Starting Gold aggregation")

        # ── Read Silver tables ────────────────────────────────────
        items_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_order_items")
            .select("order_id", "seller_id", "price", "freight_value")
        )

        sellers_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_sellers")
            .select("seller_id", "seller_city", "seller_state")
        )

        orders_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_orders")
            .select(
                "order_id", "order_status",
                "delivery_days", "delivered_on_time"
            )
            .filter(F.col("order_status").isin(["delivered", "shipped", "invoiced"]))
        )

        reviews_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_order_reviews")
            .select("order_id", "review_score")
        )

        # ── Join all tables ───────────────────────────────────────
        joined_df = (
            items_df
            .join(sellers_df, on="seller_id", how="left")
            .join(orders_df, on="order_id", how="inner")
            .join(reviews_df, on="order_id", how="left")
        )

        # ── Aggregate by seller ───────────────────────────────────
        gold_df = (
            joined_df
            .groupBy("seller_id", "seller_city", "seller_state")
            .agg(
                F.round(F.sum("price"), 2).alias("total_revenue"),
                F.countDistinct("order_id").alias("order_count"),
                F.count("*").alias("items_sold"),
                F.round(F.avg("price"), 2).alias("avg_item_price"),
                F.round(F.avg("review_score"), 2).alias("avg_review_score"),
                F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
                F.sum(
                    F.when(F.col("delivered_on_time") == True, 1).otherwise(0)
                ).alias("on_time_deliveries"),
            )
            .orderBy(F.col("total_revenue").desc())
        )

        # ── Add performance tier ──────────────────────────────────
        from pyspark.sql.window import Window
        window = Window.orderBy(F.col("total_revenue").desc())

        gold_df = (
            gold_df
            .withColumn("revenue_rank", F.rank().over(window))
            .withColumn(
                "performance_tier",
                F.when(F.col("revenue_rank") <= 100, "Top Seller")
                 .when(F.col("revenue_rank") <= 500, "Mid Tier")
                 .otherwise("Standard")
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
            f"(one row per seller)"
        )
        return row_count
