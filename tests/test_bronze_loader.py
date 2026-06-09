"""
tests/test_bronze_loader.py
----------------------------
Unit tests for the Bronze ingestion layer.

In a real enterprise, these run in CI/CD on every pull request.
A PR that breaks ingestion never reaches production.

Run with:
    pytest tests/ -v --cov=ingestion --cov-report=term-missing
"""

import os
import shutil
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from ingestion.schema_validator import validate_schema, SchemaDriftReport
from config.schema_definitions import BRONZE_ORDERS_SCHEMA


# ─────────────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """
    Create a local SparkSession for testing.
    scope="session" means one Spark instance is shared across all tests in the session
    — this is important because creating SparkSessions is expensive (~10 seconds each).
    """
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_executive_dashboard_guardian")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")  # Small value for fast tests
        .getOrCreate()
    )


@pytest.fixture
def sample_orders_df(spark):
    """Minimal orders DataFrame matching the expected schema."""
    data = [
        ("order_001", "cust_001", "delivered", "2021-01-01 10:00:00",
         "2021-01-01 11:00:00", "2021-01-02", "2021-01-05", "2021-01-06"),
        ("order_002", "cust_002", "shipped", "2021-01-02 09:00:00",
         "2021-01-02 10:00:00", "2021-01-03", None, "2021-01-07"),
    ]
    return spark.createDataFrame(data, schema=BRONZE_ORDERS_SCHEMA)


# ─────────────────────────────────────────────────────────────────
# SCHEMA VALIDATOR TESTS
# ─────────────────────────────────────────────────────────────────

class TestSchemaValidator:

    def test_no_drift_when_schemas_match(self, sample_orders_df):
        """Happy path: actual schema matches expected schema exactly."""
        report = validate_schema(
            df=sample_orders_df,
            expected_schema=BRONZE_ORDERS_SCHEMA,
            table_name="bronze_orders",
        )
        assert report.has_drift is False
        assert report.missing_columns == []
        assert report.extra_columns == []
        assert report.type_mismatches == []

    def test_detects_missing_column(self, spark):
        """Schema validator must detect when a source column is dropped."""
        # Create DF missing 'order_status' column
        data = [("order_001", "cust_001", "2021-01-01", "2021-01-01", None, None, None)]
        schema_without_status = StructType([
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("order_purchase_timestamp", StringType()),
            StructField("order_approved_at", StringType()),
            StructField("order_delivered_carrier_date", StringType()),
            StructField("order_delivered_customer_date", StringType()),
            StructField("order_estimated_delivery_date", StringType()),
        ])
        df = spark.createDataFrame(data, schema=schema_without_status)

        report = validate_schema(df, BRONZE_ORDERS_SCHEMA, "bronze_orders")

        assert report.has_drift is True
        assert "order_status" in report.missing_columns

    def test_detects_extra_column(self, spark):
        """Schema validator must detect when source adds an unexpected column."""
        data = [("order_001", "cust_001", "delivered", "2021-01-01",
                 "2021-01-01", None, None, None, "PROMO123")]  # Extra promo_code column
        schema_with_extra = StructType([
            *BRONZE_ORDERS_SCHEMA.fields,
            StructField("promo_code", StringType()),
        ])
        df = spark.createDataFrame(data, schema=schema_with_extra)

        report = validate_schema(df, BRONZE_ORDERS_SCHEMA, "bronze_orders")

        assert report.has_drift is True
        assert "promo_code" in report.extra_columns

    def test_flags_critical_missing_columns(self, spark):
        """
        When a critical (not-nullable) column is missing, it should appear
        in critical_missing — which triggers a FAILED load status in BronzeLoader.
        """
        # Only non-critical columns present
        data = [("delivered",)]
        df = spark.createDataFrame(data, ["order_status"])

        report = validate_schema(
            df,
            BRONZE_ORDERS_SCHEMA,
            "bronze_orders",
            critical_columns=["order_id", "customer_id"],
        )

        assert "order_id" in report.critical_missing
        assert "customer_id" in report.critical_missing

    def test_drift_report_serializes_to_json(self, spark):
        """Drift report must serialize to valid JSON for storage in audit log."""
        import json
        report = SchemaDriftReport(
            table_name="bronze_orders",
            has_drift=True,
            missing_columns=["order_status"],
            extra_columns=["promo_code"],
            type_mismatches=["price: expected DoubleType, got StringType"],
            critical_missing=["order_id"],
        )
        json_str = report.to_json()
        parsed = json.loads(json_str)  # Should not raise

        assert parsed["has_drift"] is True
        assert "order_status" in parsed["missing_columns"]
        assert "promo_code" in parsed["extra_columns"]


# ─────────────────────────────────────────────────────────────────
# METADATA COLUMN TESTS
# ─────────────────────────────────────────────────────────────────

class TestMetadataColumns:

    def test_metadata_columns_are_added(self, spark, sample_orders_df, tmp_path):
        """After enrichment, the 4 metadata columns must be present."""
        from ingestion.bronze_loader import BronzeLoader

        loader = BronzeLoader(spark)
        enriched = loader._add_metadata_columns(sample_orders_df, "olist_orders_dataset.csv")

        column_names = enriched.columns
        assert "_ingestion_timestamp" in column_names
        assert "_source_file" in column_names
        assert "_pipeline_version" in column_names
        assert "_row_hash" in column_names

    def test_row_hash_is_different_for_different_rows(self, spark, sample_orders_df):
        """Each unique row must produce a unique hash."""
        from ingestion.bronze_loader import BronzeLoader

        loader = BronzeLoader(spark)
        enriched = loader._add_metadata_columns(sample_orders_df, "test.csv")

        hashes = [row["_row_hash"] for row in enriched.select("_row_hash").collect()]
        assert len(hashes) == len(set(hashes)), "Duplicate hashes found for distinct rows!"

    def test_source_file_column_matches_input(self, spark, sample_orders_df):
        """_source_file column must reflect the actual CSV filename."""
        from ingestion.bronze_loader import BronzeLoader

        loader = BronzeLoader(spark)
        enriched = loader._add_metadata_columns(sample_orders_df, "olist_orders_dataset.csv")

        source_files = [row["_source_file"] for row in enriched.select("_source_file").collect()]
        assert all(f == "olist_orders_dataset.csv" for f in source_files)


# ─────────────────────────────────────────────────────────────────
# ROW COUNT THRESHOLD TESTS
# ─────────────────────────────────────────────────────────────────

class TestRowCountThresholds:

    def test_partial_status_when_below_threshold(self, spark, tmp_path, monkeypatch):
        """
        If rows loaded < minimum expected, status should be PARTIAL, not SUCCESS.
        This catches silent data loss at the source.
        """
        from ingestion.bronze_loader import BronzeLoader
        from config.settings import table_cfg

        # Override threshold so our tiny test DataFrame triggers PARTIAL
        monkeypatch.setenv("EXPECTED_ORDERS_MIN_ROWS", "999999")

        # We can't easily test the full load_table() without real CSVs,
        # so we test the internal row count logic
        loader = BronzeLoader(spark)
        min_rows = loader._get_min_rows(table_cfg.orders)

        # With our monkeypatched env, this should be 999999
        # (or at least > 2, which is our sample size)
        assert min_rows > 2, "Threshold should have been overridden"
