"""
silver/silver_order_reviews.py
-------------------------------
Transforms bronze_order_reviews into silver_order_reviews.

WHAT WE FIX IN SILVER:
1. Cast review timestamps from string to TimestampType
2. Standardize review_score (ensure it's 1-5 range)
3. Add sentiment column based on score:
   1-2 = negative, 3 = neutral, 4-5 = positive
4. Clean review text (trim whitespace)
5. Drop Bronze metadata columns

WHY THIS MATTERS FOR EXECUTIVES:
Customer satisfaction scores directly impact business decisions.
If average review score drops from 4.2 to 2.1 overnight,
that's a critical signal — but only if the data is clean and trusted.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger


class SilverOrderReviews:
    """Transforms bronze_order_reviews into silver_order_reviews."""

    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.table_name = "silver_order_reviews"

    def run(self) -> int:
        logger.info(f"[{self.table_name}] Starting Silver transformation")

        bronze_df = (
            self.spark.read
            .format("delta")
            .load(f"{self.bronze_path}/bronze_order_reviews")
        )

        silver_df = (
            bronze_df
            # Cast timestamps
            .withColumn(
                "review_creation_date",
                F.to_timestamp(F.col("review_creation_date"))
            )
            .withColumn(
                "review_answer_timestamp",
                F.to_timestamp(F.col("review_answer_timestamp"))
            )
            # Clean text fields
            .withColumn(
                "review_comment_title",
                F.trim(F.col("review_comment_title"))
            )
            .withColumn(
                "review_comment_message",
                F.trim(F.col("review_comment_message"))
            )
            # Add sentiment label based on score
            # 1-2 = negative, 3 = neutral, 4-5 = positive
            .withColumn(
                "review_sentiment",
                F.when(F.col("review_score") >= 4, "positive")
                 .when(F.col("review_score") == 3, "neutral")
                 .when(F.col("review_score") <= 2, "negative")
                 .otherwise("unknown")
            )
            # Flag if customer left a written comment
            .withColumn(
                "has_comment",
                F.when(
                    F.col("review_comment_message").isNotNull() &
                    (F.length(F.trim(F.col("review_comment_message"))) > 0),
                    True
                ).otherwise(False)
            )
            # How many days did it take to answer the review?
            .withColumn(
                "days_to_answer",
                F.datediff(
                    F.col("review_answer_timestamp"),
                    F.col("review_creation_date")
                )
            )
        )

        # Filter out invalid scores (must be 1-5)
        before = silver_df.count()
        silver_df = silver_df.filter(
            F.col("review_id").isNotNull() &
            F.col("order_id").isNotNull() &
            F.col("review_score").between(1, 5)
        )
        dropped = before - silver_df.count()
        if dropped > 0:
            logger.warning(
                f"[{self.table_name}] Dropped {dropped} rows with "
                f"invalid review scores or null IDs"
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
