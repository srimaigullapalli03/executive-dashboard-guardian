"""
silver/silver_products.py
--------------------------
Transforms bronze_products into silver_products.

WHAT WE FIX IN SILVER:
1. Standardize product_category_name (lowercase, trim)
2. Fill null category names with 'uncategorized'
3. Drop Bronze metadata columns
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger


class SilverProducts:
    """Transforms bronze_products into silver_products."""

    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.table_name = "silver_products"

    def run(self) -> int:
        logger.info(f"[{self.table_name}] Starting Silver transformation")

        bronze_df = (
            self.spark.read
            .format("delta")
            .load(f"{self.bronze_path}/bronze_products")
        )

        silver_df = (
            bronze_df
            # Standardize category name
            .withColumn(
                "product_category_name",
                F.lower(F.trim(F.col("product_category_name")))
            )
            # Fill null categories so joins don't drop products
            .withColumn(
                "product_category_name",
                F.coalesce(
                    F.col("product_category_name"),
                    F.lit("uncategorized")
                )
            )
            # Calculate product volume in cubic cm
            .withColumn(
                "product_volume_cm3",
                F.round(
                    F.col("product_length_cm") *
                    F.col("product_height_cm") *
                    F.col("product_width_cm"), 2
                )
            )
        )

        # Filter critical nulls
        silver_df = silver_df.filter(F.col("product_id").isNotNull())

        # Drop metadata columns
        silver_df = silver_df.drop(
            "_ingestion_timestamp", "_source_file",
            "_pipeline_version", "_row_hash"
        )

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
