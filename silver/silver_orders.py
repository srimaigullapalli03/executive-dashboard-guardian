"""
silver/silver_orders.py
------------------------
Transforms bronze_orders into silver_orders.

WHAT WE FIX IN SILVER:
1. Cast timestamp strings → proper TimestampType
2. Standardize order_status values (lowercase, trim whitespace)
3. Add derived columns (order_year, order_month, order_day_of_week)
4. Drop the metadata columns that were only needed for Bronze validation
5. Filter out any rows with null order_id or customer_id (critical fields)

WHY THIS MATTERS:
Bronze = raw data, stored as-is from source
Silver = clean, typed, trusted data ready for business logic

Power BI cannot do date filtering on a text column like "2017-10-02 10:56:33"
It needs a proper TimestampType. That conversion happens here in Silver.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from loguru import logger


class SilverOrders:
    """Transforms bronze_orders into silver_orders."""

    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.table_name = "silver_orders"

    def run(self) -> int:
        """
        Read from Bronze, clean, transform, write to Silver.
        Returns number of rows written.
        """
        logger.info(f"[{self.table_name}] Starting Silver transformation")

        # ── Step 1: Read Bronze ───────────────────────────────────
        bronze_df = (
            self.spark.read
            .format("delta")
            .load(f"{self.bronze_path}/bronze_orders")
        )
        logger.info(f"[{self.table_name}] Bronze rows read: {bronze_df.count():,}")

        # ── Step 2: Cast timestamps from string to TimestampType ──
        silver_df = (
            bronze_df
            .withColumn(
                "order_purchase_timestamp",
                F.to_timestamp(F.col("order_purchase_timestamp"))
            )
            .withColumn(
                "order_approved_at",
                F.to_timestamp(F.col("order_approved_at"))
            )
            .withColumn(
                "order_delivered_carrier_date",
                F.to_timestamp(F.col("order_delivered_carrier_date"))
            )
            .withColumn(
                "order_delivered_customer_date",
                F.to_timestamp(F.col("order_delivered_customer_date"))
            )
            .withColumn(
                "order_estimated_delivery_date",
                F.to_timestamp(F.col("order_estimated_delivery_date"))
            )
        )

        # ── Step 3: Standardize order_status ─────────────────────
        # Trim whitespace and convert to lowercase for consistency
        silver_df = silver_df.withColumn(
            "order_status",
            F.lower(F.trim(F.col("order_status")))
        )

        # ── Step 4: Add derived date columns ─────────────────────
        # These make Power BI filtering much easier
        silver_df = (
            silver_df
            .withColumn("order_year",
                F.year(F.col("order_purchase_timestamp")))
            .withColumn("order_month",
                F.month(F.col("order_purchase_timestamp")))
            .withColumn("order_quarter",
                F.quarter(F.col("order_purchase_timestamp")))
            .withColumn("order_day_of_week",
                F.dayofweek(F.col("order_purchase_timestamp")))
            .withColumn("order_date",
                F.to_date(F.col("order_purchase_timestamp")))
            # Delivery time in days (how long did delivery take?)
            .withColumn("delivery_days",
                F.datediff(
                    F.col("order_delivered_customer_date"),
                    F.col("order_purchase_timestamp")
                )
            )
            # Was the order delivered on time?
            .withColumn("delivered_on_time",
                F.when(
                    F.col("order_delivered_customer_date") <=
                    F.col("order_estimated_delivery_date"),
                    True
                ).otherwise(False)
            )
        )

        # ── Step 5: Filter out critical null rows ─────────────────
        before_filter = silver_df.count()
        silver_df = silver_df.filter(
            F.col("order_id").isNotNull() &
            F.col("customer_id").isNotNull()
        )
        after_filter = silver_df.count()
        dropped = before_filter - after_filter
        if dropped > 0:
            logger.warning(
                f"[{self.table_name}] Dropped {dropped} rows with null "
                f"order_id or customer_id"
            )

        # ── Step 6: Drop Bronze metadata columns ──────────────────
        # These were needed for Phase 1 validation but not for Silver
        cols_to_drop = [
            "_ingestion_timestamp",
            "_source_file",
            "_pipeline_version",
            "_row_hash"
        ]
        silver_df = silver_df.drop(*cols_to_drop)

        # ── Step 7: Write to Silver Delta table ───────────────────
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
            f"[{self.table_name}] Silver transformation complete. "
            f"{row_count:,} rows written to {output_path}"
        )
        return row_count
