"""
silver/silver_runner.py
------------------------
Orchestrates all Silver layer transformations.
Runs all 8 table transformers in sequence and reports results.
"""

from pyspark.sql import SparkSession
from loguru import logger

from config.settings import path_cfg
from silver.silver_orders import SilverOrders
from silver.silver_order_items import SilverOrderItems
from silver.silver_payments import SilverPayments
from silver.silver_customers import SilverCustomers
from silver.silver_products import SilverProducts
from silver.silver_sellers import SilverSellers
from silver.silver_order_reviews import SilverOrderReviews
from silver.silver_geolocation import SilverGeolocation


class SilverRunner:
    """Runs all 8 Silver transformations in sequence."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bronze_path = path_cfg.bronze_path
        self.silver_path = f"{path_cfg.delta_base_path}/silver"

        self.transformers = [
            SilverOrders(spark,        self.bronze_path, self.silver_path),
            SilverOrderItems(spark,    self.bronze_path, self.silver_path),
            SilverPayments(spark,      self.bronze_path, self.silver_path),
            SilverCustomers(spark,     self.bronze_path, self.silver_path),
            SilverProducts(spark,      self.bronze_path, self.silver_path),
            SilverSellers(spark,       self.bronze_path, self.silver_path),
            SilverOrderReviews(spark,  self.bronze_path, self.silver_path),
            SilverGeolocation(spark,   self.bronze_path, self.silver_path),
        ]

    def run_all(self) -> dict:
        """Run all Silver transformations. Returns summary dict."""
        logger.info("=" * 60)
        logger.info("SILVER TRANSFORMATION PIPELINE — STARTING")
        logger.info("=" * 60)

        results = {}

        for transformer in self.transformers:
            table_name = transformer.table_name
            try:
                row_count = transformer.run()
                results[table_name] = {
                    "status": "SUCCESS",
                    "rows": row_count
                }
            except Exception as e:
                logger.error(f"[{table_name}] FAILED: {e}")
                results[table_name] = {
                    "status": "FAILED",
                    "rows": 0,
                    "error": str(e)
                }

        logger.info("=" * 60)
        logger.info("SILVER TRANSFORMATION PIPELINE — COMPLETE")
        logger.info("=" * 60)

        return results
