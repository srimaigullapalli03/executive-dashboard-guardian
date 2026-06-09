"""
gold/gold_customer_satisfaction.py
------------------------------------
Creates the gold_customer_satisfaction table.

WHAT THIS TABLE ANSWERS:
"How satisfied are our customers, by category and month?"

This powers the customer satisfaction scorecard in Power BI.
Executives use this to track whether product quality is improving.

HOW IT WORKS:
Join silver_order_reviews + silver_orders + silver_order_items + silver_products
Group by product_category and month
Calculate average review score, positive/negative/neutral counts
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger


class GoldCustomerSatisfaction:
    """Builds gold_customer_satisfaction aggregation table."""

    def __init__(self, spark: SparkSession, silver_path: str, gold_path: str):
        self.spark = spark
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.table_name = "gold_customer_satisfaction"

    def run(self) -> int:
        logger.info(f"[{self.table_name}] Starting Gold aggregation")

        # ── Read Silver tables ────────────────────────────────────
        reviews_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_order_reviews")
            .select(
                "order_id",
                "review_score",
                "review_sentiment",
                "has_comment",
                "days_to_answer",
            )
        )

        orders_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_orders")
            .select("order_id", "order_year", "order_month", "order_quarter")
        )

        items_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_order_items")
            .select("order_id", "product_id")
        )

        products_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_products")
            .select("product_id", "product_category_name")
        )

        # ── Join all tables ───────────────────────────────────────
        joined_df = (
            reviews_df
            .join(orders_df, on="order_id", how="left")
            .join(items_df, on="order_id", how="left")
            .join(products_df, on="product_id", how="left")
        )

        # ── Aggregate by category and month ───────────────────────
        gold_df = (
            joined_df
            .groupBy(
                "product_category_name",
                "order_year",
                "order_month",
                "order_quarter",
            )
            .agg(
                F.round(F.avg("review_score"), 2).alias("avg_review_score"),
                F.count("*").alias("total_reviews"),
                F.sum(
                    F.when(F.col("review_sentiment") == "positive", 1).otherwise(0)
                ).alias("positive_reviews"),
                F.sum(
                    F.when(F.col("review_sentiment") == "neutral", 1).otherwise(0)
                ).alias("neutral_reviews"),
                F.sum(
                    F.when(F.col("review_sentiment") == "negative", 1).otherwise(0)
                ).alias("negative_reviews"),
                F.sum(
                    F.when(F.col("has_comment") == True, 1).otherwise(0)
                ).alias("reviews_with_comments"),
                F.round(F.avg("days_to_answer"), 1).alias("avg_days_to_answer"),
            )
            .orderBy("order_year", "order_month",
                     F.col("avg_review_score").desc())
        )

        # ── Add satisfaction label ────────────────────────────────
        gold_df = gold_df.withColumn(
            "satisfaction_level",
            F.when(F.col("avg_review_score") >= 4.0, "High")
             .when(F.col("avg_review_score") >= 3.0, "Medium")
             .otherwise("Low")
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
            f"[{self.table_name}] Complete. {row_count:,} rows written."
        )
        return row_count
