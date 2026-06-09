"""
config/schema_definitions.py
-----------------------------
Expected PySpark schemas for every Bronze table.

WHY THIS MATTERS:
When a source system changes a column type (e.g., price goes from string to float,
or a new column is added), this file is your early-warning system.
Schema drift is one of the most common causes of silent data corruption in pipelines.

Design: Schemas are defined with StructType so they can be passed directly
to spark.read.schema() — this enforces types at ingestion rather than discovering
problems downstream when aggregations start failing.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType, LongType
)

# ─────────────────────────────────────────
# BRONZE SCHEMAS
# All timestamps are kept as StringType here intentionally.
# Raw Bronze = ingest as-is. Type casting happens in Silver.
# This prevents ingestion failures from malformed timestamps
# while still capturing the data for investigation.
# ─────────────────────────────────────────

BRONZE_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("order_status", StringType(), nullable=True),
    StructField("order_purchase_timestamp", StringType(), nullable=True),
    StructField("order_approved_at", StringType(), nullable=True),
    StructField("order_delivered_carrier_date", StringType(), nullable=True),
    StructField("order_delivered_customer_date", StringType(), nullable=True),
    StructField("order_estimated_delivery_date", StringType(), nullable=True),
])

BRONZE_ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("order_item_id", IntegerType(), nullable=True),
    StructField("product_id", StringType(), nullable=False),
    StructField("seller_id", StringType(), nullable=False),
    StructField("shipping_limit_date", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("freight_value", DoubleType(), nullable=True),
])

BRONZE_ORDER_PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("payment_sequential", IntegerType(), nullable=True),
    StructField("payment_type", StringType(), nullable=True),
    StructField("payment_installments", IntegerType(), nullable=True),
    StructField("payment_value", DoubleType(), nullable=True),
])

BRONZE_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("customer_unique_id", StringType(), nullable=False),
    StructField("customer_zip_code_prefix", StringType(), nullable=True),
    StructField("customer_city", StringType(), nullable=True),
    StructField("customer_state", StringType(), nullable=True),
])

BRONZE_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_category_name", StringType(), nullable=True),
    StructField("product_name_lenght", IntegerType(), nullable=True),   # Note: intentional typo from source data
    StructField("product_description_lenght", IntegerType(), nullable=True),
    StructField("product_photos_qty", IntegerType(), nullable=True),
    StructField("product_weight_g", DoubleType(), nullable=True),
    StructField("product_length_cm", DoubleType(), nullable=True),
    StructField("product_height_cm", DoubleType(), nullable=True),
    StructField("product_width_cm", DoubleType(), nullable=True),
])

# ─────────────────────────────────────────
# AUDIT LOG SCHEMA
# Written by our pipeline, not from source.
# ─────────────────────────────────────────

AUDIT_LOG_SCHEMA = StructType([
    StructField("audit_id", StringType(), nullable=False),
    StructField("pipeline_name", StringType(), nullable=False),
    StructField("pipeline_version", StringType(), nullable=True),
    StructField("table_name", StringType(), nullable=False),
    StructField("source_file", StringType(), nullable=True),
    StructField("load_status", StringType(), nullable=False),       # SUCCESS | FAILED | PARTIAL
    StructField("rows_loaded", LongType(), nullable=True),
    StructField("rows_expected_min", LongType(), nullable=True),
    StructField("schema_drift_detected", StringType(), nullable=True),  # JSON list of drifted columns
    StructField("error_message", StringType(), nullable=True),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    StructField("environment", StringType(), nullable=True),
])

# Lookup map for easy access by table name
SCHEMA_MAP = {
    "bronze_orders": BRONZE_ORDERS_SCHEMA,
    "bronze_order_items": BRONZE_ORDER_ITEMS_SCHEMA,
    "bronze_order_payments": BRONZE_ORDER_PAYMENTS_SCHEMA,
    "bronze_customers": BRONZE_CUSTOMERS_SCHEMA,
    "bronze_products": BRONZE_PRODUCTS_SCHEMA,
    "bronze_ingestion_audit": AUDIT_LOG_SCHEMA,
}


# ─────────────────────────────────────────
# ADDITIONAL BRONZE SCHEMAS (sellers, reviews, geolocation)
# ─────────────────────────────────────────

BRONZE_SELLERS_SCHEMA = StructType([
    StructField("seller_id",                   StringType(), nullable=False),
    StructField("seller_zip_code_prefix",      StringType(), nullable=True),
    StructField("seller_city",                 StringType(), nullable=True),
    StructField("seller_state",                StringType(), nullable=True),
])

BRONZE_ORDER_REVIEWS_SCHEMA = StructType([
    StructField("review_id",                   StringType(), nullable=False),
    StructField("order_id",                    StringType(), nullable=False),
    StructField("review_score",                IntegerType(), nullable=True),
    StructField("review_comment_title",        StringType(), nullable=True),
    StructField("review_comment_message",      StringType(), nullable=True),
    StructField("review_creation_date",        StringType(), nullable=True),
    StructField("review_answer_timestamp",     StringType(), nullable=True),
])

BRONZE_GEOLOCATION_SCHEMA = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), nullable=False),
    StructField("geolocation_lat",             DoubleType(), nullable=True),
    StructField("geolocation_lng",             DoubleType(), nullable=True),
    StructField("geolocation_city",            StringType(), nullable=True),
    StructField("geolocation_state",           StringType(), nullable=True),
])

# Update SCHEMA_MAP to include all 8 tables
SCHEMA_MAP = {
    "bronze_orders":          BRONZE_ORDERS_SCHEMA,
    "bronze_order_items":     BRONZE_ORDER_ITEMS_SCHEMA,
    "bronze_order_payments":  BRONZE_ORDER_PAYMENTS_SCHEMA,
    "bronze_customers":       BRONZE_CUSTOMERS_SCHEMA,
    "bronze_products":        BRONZE_PRODUCTS_SCHEMA,
    "bronze_sellers":         BRONZE_SELLERS_SCHEMA,
    "bronze_order_reviews":   BRONZE_ORDER_REVIEWS_SCHEMA,
    "bronze_geolocation":     BRONZE_GEOLOCATION_SCHEMA,
    "bronze_ingestion_audit": AUDIT_LOG_SCHEMA,
}
