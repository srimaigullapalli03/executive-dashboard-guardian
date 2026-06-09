"""
gold/gold_runner.py
--------------------
Orchestrates all Gold layer aggregations.
Runs all 4 Gold tables in sequence and reports results.
"""

from pyspark.sql import SparkSession
from loguru import logger

from config.settings import path_cfg
from gold.gold_daily_revenue import GoldDailyRevenue
from gold.gold_revenue_by_category import GoldRevenueByCategory
from gold.gold_revenue_by_state import GoldRevenueByState
from gold.gold_customer_satisfaction import GoldCustomerSatisfaction
from gold.gold_seller_performance import GoldSellerPerformance


class GoldRunner:
    """Runs all Gold aggregations in sequence."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.silver_path = f"{path_cfg.delta_base_path}/silver"
        self.gold_path = f"{path_cfg.delta_base_path}/gold"

        self.aggregators = [
            GoldDailyRevenue(spark,          self.silver_path, self.gold_path),
            GoldRevenueByCategory(spark,     self.silver_path, self.gold_path),
            GoldRevenueByState(spark,        self.silver_path, self.gold_path),
            GoldCustomerSatisfaction(spark,  self.silver_path, self.gold_path),
            GoldSellerPerformance(spark,     self.silver_path, self.gold_path),
        ]

    def run_all(self) -> dict:
        """Run all Gold aggregations. Returns summary dict."""
        logger.info("=" * 60)
        logger.info("GOLD AGGREGATION PIPELINE — STARTING")
        logger.info("=" * 60)

        results = {}

        for aggregator in self.aggregators:
            table_name = aggregator.table_name
            try:
                row_count = aggregator.run()
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
        logger.info("GOLD AGGREGATION PIPELINE — COMPLETE")
        logger.info("=" * 60)

        return results
