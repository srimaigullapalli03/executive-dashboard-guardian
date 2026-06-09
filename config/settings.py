"""
config/settings.py
------------------
Centralized configuration for Executive Dashboard Guardian.
All environment-specific values live here — never hardcoded in business logic.

Design principle: A junior engineer changing environments (dev → prod)
should only need to update this file or the .env, nothing else.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()


@dataclass
class PipelineConfig:
    """Top-level pipeline metadata."""
    env: str = os.getenv("PIPELINE_ENV", "development")
    version: str = os.getenv("PIPELINE_VERSION", "1.0.0")
    pipeline_name: str = "executive_dashboard_guardian"


@dataclass
class PathConfig:
    """All file system paths in one place."""
    raw_data_path: str = os.getenv("RAW_DATA_PATH", "./data/raw")
    delta_base_path: str = os.getenv("DELTA_BASE_PATH", "./data/delta")
    log_path: str = "./logs"

    # Derived delta table paths
    @property
    def bronze_path(self) -> str:
        return f"{self.delta_base_path}/bronze"

    @property
    def audit_path(self) -> str:
        return f"{self.delta_base_path}/audit"


@dataclass
class TableConfig:
    """Delta table names — centralised so renaming is a one-line change."""
    orders: str = os.getenv("BRONZE_ORDERS_TABLE", "bronze_orders")
    order_items: str = os.getenv("BRONZE_ORDER_ITEMS_TABLE", "bronze_order_items")
    payments: str = os.getenv("BRONZE_PAYMENTS_TABLE", "bronze_order_payments")
    customers: str = os.getenv("BRONZE_CUSTOMERS_TABLE", "bronze_customers")
    products: str = os.getenv("BRONZE_PRODUCTS_TABLE", "bronze_products")
    sellers: str = os.getenv("BRONZE_SELLERS_TABLE", "bronze_sellers")
    reviews: str = os.getenv("BRONZE_REVIEWS_TABLE", "bronze_order_reviews")
    geolocation: str = os.getenv("BRONZE_GEOLOCATION_TABLE", "bronze_geolocation")
    audit_log: str = os.getenv("BRONZE_AUDIT_TABLE", "bronze_ingestion_audit")


@dataclass
class RowCountThresholds:
    """
    Minimum expected row counts per table.
    If a load produces FEWER rows than this, it's flagged as PARTIAL or FAILED.
    """
    orders: int = int(os.getenv("EXPECTED_ORDERS_MIN_ROWS", 90000))
    order_items: int = int(os.getenv("EXPECTED_ORDER_ITEMS_MIN_ROWS", 110000))
    payments: int = 100000
    customers: int = 90000
    products: int = 30000
    sellers: int = 3000
    reviews: int = 90000
    geolocation: int = 1000000


# Source file mapping: table_name -> csv filename
SOURCE_FILE_MAP: Dict[str, str] = {
    # Core transaction tables — needed for revenue dashboards
    "bronze_orders": "olist_orders_dataset.csv",
    "bronze_order_items": "olist_order_items_dataset.csv",
    "bronze_order_payments": "olist_order_payments_dataset.csv",
    # Core entity tables — needed for joins and enrichment
    "bronze_customers": "olist_customers_dataset.csv",
    "bronze_products": "olist_products_dataset.csv",
    # Supporting tables — enrichment and analysis
    "bronze_sellers": "olist_sellers_dataset.csv",
    "bronze_order_reviews": "olist_order_reviews_dataset.csv",
    "bronze_geolocation": "olist_geolocation_dataset.csv",
}

# Columns that must NEVER be null — used by null-check validators in Phase 3
CRITICAL_NOT_NULL_COLUMNS: Dict[str, List[str]] = {
    "bronze_orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
    "bronze_order_items": ["order_id", "product_id", "seller_id", "price"],
    "bronze_order_payments": ["order_id", "payment_type", "payment_value"],
    "bronze_customers": ["customer_id", "customer_unique_id"],
    "bronze_products": ["product_id"],
    "bronze_sellers": ["seller_id"],
    "bronze_order_reviews": ["review_id", "order_id"],
    "bronze_geolocation": ["geolocation_zip_code_prefix"],
}

# Instantiate global config objects for import
pipeline_cfg = PipelineConfig()
path_cfg = PathConfig()
table_cfg = TableConfig()
row_thresholds = RowCountThresholds()


@dataclass
class DataQualityConfig:
    """
    Thresholds for data quality checks.
    Tune these based on your business requirements.
    """
    staleness_threshold_hours: int = 24      # Flag if data older than 24 hours
    revenue_zscore_threshold: float = 3.0    # Flag if Z-score exceeds 3
    volume_drop_pct_threshold: float = 50.0  # Flag if volume drops more than 50%
    null_pct_critical_threshold: float = 1.0 # Flag as CRITICAL if nulls > 1%


dq_cfg = DataQualityConfig()
