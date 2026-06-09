"""
silver/silver_geolocation.py
-----------------------------
Transforms bronze_geolocation into silver_geolocation.

WHAT WE FIX IN SILVER:
1. Standardize city and state names (lowercase, trim)
2. Filter out invalid coordinates
   (Brazil lat range: -35 to 5, lng range: -74 to -34)
3. Drop duplicate zip codes (keep one representative location per zip)
4. Drop Bronze metadata columns

WHY THIS MATTERS:
Geolocation data is used to plot revenue on a map in Power BI.
Invalid coordinates (lat=0, lng=0) would place orders in the ocean.
Duplicate zip codes would skew regional aggregations.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger


class SilverGeolocation:
    """Transforms bronze_geolocation into silver_geolocation."""

    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.table_name = "silver_geolocation"

    def run(self) -> int:
        logger.info(f"[{self.table_name}] Starting Silver transformation")

        bronze_df = (
            self.spark.read
            .format("delta")
            .load(f"{self.bronze_path}/bronze_geolocation")
        )

        silver_df = (
            bronze_df
            # Standardize city and state
            .withColumn("geolocation_city",
                F.lower(F.trim(F.col("geolocation_city"))))
            .withColumn("geolocation_state",
                F.upper(F.trim(F.col("geolocation_state"))))
            .withColumn("geolocation_zip_code_prefix",
                F.trim(F.col("geolocation_zip_code_prefix")))
        )

        # Filter out invalid coordinates for Brazil
        # Valid Brazil range: lat between -35 and 5, lng between -74 and -34
        before = silver_df.count()
        silver_df = silver_df.filter(
            F.col("geolocation_lat").between(-35, 5) &
            F.col("geolocation_lng").between(-74, -34) &
            F.col("geolocation_zip_code_prefix").isNotNull()
        )
        dropped = before - silver_df.count()
        if dropped > 0:
            logger.warning(
                f"[{self.table_name}] Dropped {dropped} rows with "
                f"invalid coordinates"
            )

        # Deduplicate — keep one row per zip code
        # Use average lat/lng for each zip as the representative point
        silver_df = (
            silver_df
            .groupBy(
                "geolocation_zip_code_prefix",
                "geolocation_city",
                "geolocation_state"
            )
            .agg(
                F.round(F.avg("geolocation_lat"), 6).alias("geolocation_lat"),
                F.round(F.avg("geolocation_lng"), 6).alias("geolocation_lng"),
            )
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
