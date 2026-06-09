"""
silver/silver_payments.py
--------------------------
Transforms bronze_order_payments into silver_order_payments.

WHAT WE FIX IN SILVER:
1. Standardize payment_type values (lowercase, trim)
2. Flag high-value payments (useful for fraud detection later)
3. Filter rows with null payment_value
4. Drop Bronze metadata columns
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger


class SilverPayments:
    """Transforms bronze_order_payments into silver_order_payments."""

    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.table_name = "silver_order_payments"

    def run(self) -> int:
        logger.info(f"[{self.table_name}] Starting Silver transformation")

        bronze_df = (
            self.spark.read
            .format("delta")
            .load(f"{self.bronze_path}/bronze_order_payments")
        )

        silver_df = (
            bronze_df
            # Standardize payment type text
            .withColumn(
                "payment_type",
                F.lower(F.trim(F.col("payment_type")))
            )
            # Round payment value to 2 decimal places
            .withColumn(
                "payment_value",
                F.round(F.col("payment_value"), 2)
            )
            # Flag unusually high payments (>$1000) for review
            .withColumn(
                "is_high_value",
                F.when(F.col("payment_value") > 1000, True).otherwise(False)
            )
            # Flag installment payments
            .withColumn(
                "is_installment",
                F.when(F.col("payment_installments") > 1, True).otherwise(False)
            )
        )

        # Filter null payment values — can't trust revenue without this
        before = silver_df.count()
        silver_df = silver_df.filter(F.col("payment_value").isNotNull())
        dropped = before - silver_df.count()
        if dropped > 0:
            logger.warning(
                f"[{self.table_name}] Dropped {dropped} rows with null payment_value"
            )

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
