"""
silver/silver_sellers.py
-------------------------
Transforms bronze_sellers into silver_sellers.

WHAT WE FIX IN SILVER:
1. Standardize city and state names (lowercase, trim)
2. Drop Bronze metadata columns
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger


class SilverSellers:
    """Transforms bronze_sellers into silver_sellers."""

    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.table_name = "silver_sellers"

    def run(self) -> int:
        logger.info(f"[{self.table_name}] Starting Silver transformation")

        bronze_df = (
            self.spark.read
            .format("delta")
            .load(f"{self.bronze_path}/bronze_sellers")
        )

        silver_df = (
            bronze_df
            .withColumn("seller_city",
                F.lower(F.trim(F.col("seller_city"))))
            .withColumn("seller_state",
                F.upper(F.trim(F.col("seller_state"))))
            .withColumn("seller_zip_code_prefix",
                F.trim(F.col("seller_zip_code_prefix")))
        )

        # Filter critical nulls
        silver_df = silver_df.filter(F.col("seller_id").isNotNull())

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
