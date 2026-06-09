"""
gold/gold_revenue_by_category.py
---------------------------------
Creates the gold_revenue_by_category table.

WHAT THIS TABLE ANSWERS:
"Which product categories generate the most revenue?"

This powers the category breakdown bar chart in Power BI.
Executives use this to decide which product lines to invest in.

HOW IT WORKS:
Join silver_order_items + silver_orders + silver_products
Group by product_category_name
Calculate total revenue, order count, avg price per category
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger


class GoldRevenueByCategory:
    """Builds gold_revenue_by_category aggregation table."""

    def __init__(self, spark: SparkSession, silver_path: str, gold_path: str):
        self.spark = spark
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.table_name = "gold_revenue_by_category"

    def run(self) -> int:
        logger.info(f"[{self.table_name}] Starting Gold aggregation")

        # ── Read Silver tables ────────────────────────────────────
        items_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_order_items")
            .select("order_id", "product_id", "price", "freight_value")
        )

        products_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_products")
            .select("product_id", "product_category_name")
        )

        orders_df = (
            self.spark.read.format("delta")
            .load(f"{self.silver_path}/silver_orders")
            .select("order_id", "order_status", "order_year", "order_month")
            .filter(F.col("order_status").isin(["delivered", "shipped", "invoiced"]))
        )

        # ── Join all three ────────────────────────────────────────
        joined_df = (
            items_df
            .join(products_df, on="product_id", how="left")
            .join(orders_df, on="order_id", how="inner")
        )

        # ── Aggregate by category ─────────────────────────────────
        gold_df = (
            joined_df
            .groupBy("product_category_name")
            .agg(
                F.round(F.sum("price"), 2).alias("total_revenue"),
                F.countDistinct("order_id").alias("order_count"),
                F.count("product_id").alias("items_sold"),
                F.round(F.avg("price"), 2).alias("avg_item_price"),
                F.round(F.sum("freight_value"), 2).alias("total_freight"),
            )
            .orderBy(F.col("total_revenue").desc())
        )

        # ── Add revenue rank ──────────────────────────────────────
        from pyspark.sql.window import Window
        window = Window.orderBy(F.col("total_revenue").desc())
        gold_df = gold_df.withColumn(
            "revenue_rank",
            F.rank().over(window)
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
            f"(one row per product category)"
        )
        return row_count
