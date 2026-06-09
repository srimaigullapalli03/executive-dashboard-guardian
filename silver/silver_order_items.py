"""
silver/silver_order_items.py
-----------------------------
Transforms bronze_order_items into silver_order_items.

WHAT WE FIX IN SILVER:
1. Cast shipping_limit_date string → proper TimestampType
2. Add total_item_value column (price + freight_value)
3. Filter out rows with null price (can't calculate revenue without price)
4. Drop Bronze metadata columns
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from loguru import logger


class SilverOrderItems:
    """Transforms bronze_order_items into silver_order_items."""

    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.table_name = "silver_order_items"

    def run(self) -> int:
        logger.info(f"[{self.table_name}] Starting Silver transformation")

        # ── Read Bronze ───────────────────────────────────────────
        bronze_df = (
            self.spark.read
            .format("delta")
            .load(f"{self.bronze_path}/bronze_order_items")
        )

        # ── Transform ─────────────────────────────────────────────
        silver_df = (
            bronze_df
            # Cast timestamp
            .withColumn(
                "shipping_limit_date",
                F.to_timestamp(F.col("shipping_limit_date"))
            )
            # Total value per item line = price + shipping
            .withColumn(
                "total_item_value",
                F.round(
                    F.col("price") + F.col("freight_value"), 2
                )
            )
            # Flag free shipping (freight = 0)
            .withColumn(
                "is_free_shipping",
                F.when(F.col("freight_value") == 0, True).otherwise(False)
            )
        )

        # ── Filter nulls on critical revenue column ───────────────
        before = silver_df.count()
        silver_df = silver_df.filter(F.col("price").isNotNull())
        dropped = before - silver_df.count()
        if dropped > 0:
            logger.warning(
                f"[{self.table_name}] Dropped {dropped} rows with null price"
            )

        # ── Drop metadata columns ─────────────────────────────────
        silver_df = silver_df.drop(
            "_ingestion_timestamp", "_source_file",
            "_pipeline_version", "_row_hash"
        )

        # ── Write Silver ──────────────────────────────────────────
        output_path = f"{self.silver_path}/{self.table_name}"
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(output_path)
        )

        row_count = silver_df.count()
        logger.success(
            f"[{self.table_name}] Complete. {row_count:,} rows written."
        )
        return row_count
