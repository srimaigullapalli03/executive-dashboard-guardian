"""
ingestion/bronze_loader.py
---------------------------
Core Bronze layer ingestion engine for Executive Dashboard Guardian.

RESPONSIBILITIES:
1. Read raw CSV files from the data/raw directory
2. Inject metadata columns (_ingestion_timestamp, _source_file, _row_hash)
3. Validate schema against expected definitions
4. Write to Delta Lake in Bronze layer (append or overwrite based on mode)
5. Record every outcome in the audit log

METADATA COLUMNS WE ADD (never in the source CSV):
  _ingestion_timestamp  → When this record was loaded into our platform
  _source_file          → Which CSV file it came from (for lineage)
  _row_hash             → MD5 hash of all columns (used for dedup detection in Phase 2)
  _pipeline_version     → Version of the code that produced this record

These columns are prefixed with _ by convention — that's how data teams signal
"this column was added by the platform, not from the source system."
"""

import os
import hashlib
from datetime import datetime, timezone
from typing import Optional, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from loguru import logger

from config.settings import (
    path_cfg, table_cfg, pipeline_cfg,
    SOURCE_FILE_MAP, CRITICAL_NOT_NULL_COLUMNS, row_thresholds
)
from config.schema_definitions import SCHEMA_MAP
from ingestion.schema_validator import validate_schema, SchemaDriftReport
from ingestion.audit_logger import AuditLogger


class BronzeLoader:
    """
    Loads raw CSV files into Bronze Delta tables with full metadata and validation.

    Usage:
        loader = BronzeLoader(spark)
        loader.run_full_pipeline()        # Load all tables
        loader.load_table("bronze_orders")  # Load one table
    """

    def __init__(self, spark: SparkSession, write_mode: str = "overwrite"):
        """
        Args:
            spark: Active SparkSession
            write_mode: "overwrite" for full reload, "append" for incremental.
                        For Bronze, we typically use "overwrite" on initial load
                        and "append" for daily incremental loads.
        """
        self.spark = spark
        self.write_mode = write_mode
        self.audit_logger = AuditLogger(spark)
        self.ingestion_timestamp = datetime.now(timezone.utc)
        logger.info(
            f"BronzeLoader initialized | env={pipeline_cfg.env} | "
            f"version={pipeline_cfg.version} | mode={write_mode}"
        )

    # ─────────────────────────────────────────────────────────────
    # PUBLIC METHODS
    # ─────────────────────────────────────────────────────────────

    def run_full_pipeline(self) -> dict:
        """
        Load ALL Bronze tables in sequence.
        Returns a summary dict of {table_name: load_status}.
        """
        logger.info("=" * 60)
        logger.info("BRONZE INGESTION PIPELINE — STARTING")
        logger.info("=" * 60)

        results = {}
        for table_name in SOURCE_FILE_MAP.keys():
            status = self.load_table(table_name)
            results[table_name] = status

        self._log_pipeline_summary(results)
        return results

    def load_table(self, table_name: str) -> str:
        """
        Load a single Bronze table from its source CSV.

        Returns:
            "SUCCESS" | "PARTIAL" | "FAILED"
        """
        source_file = SOURCE_FILE_MAP.get(table_name)
        if not source_file:
            raise ValueError(f"Unknown table: {table_name}. Check SOURCE_FILE_MAP in settings.py")

        source_path = os.path.join(path_cfg.raw_data_path, source_file)
        delta_path = os.path.join(path_cfg.bronze_path, table_name)
        expected_schema = SCHEMA_MAP.get(table_name)
        critical_cols = CRITICAL_NOT_NULL_COLUMNS.get(table_name, [])
        min_rows = self._get_min_rows(table_name)

        logger.info(f"[{table_name}] Starting load from {source_file}")

        try:
            # ── STEP 1: Read raw CSV ──────────────────────────────
            raw_df = self._read_csv(source_path, expected_schema)
            logger.info(f"[{table_name}] CSV read complete. Raw count (pre-action): pending")

            # ── STEP 2: Validate schema ───────────────────────────
            drift_report: SchemaDriftReport = validate_schema(
                df=raw_df,
                expected_schema=expected_schema,
                table_name=table_name,
                critical_columns=critical_cols,
            )

            # Hard stop if critical columns are missing
            if drift_report.critical_missing:
                raise ValueError(
                    f"Critical columns missing from source: {drift_report.critical_missing}. "
                    f"Cannot safely load {table_name}."
                )

            # ── STEP 3: Add metadata columns ──────────────────────
            enriched_df = self._add_metadata_columns(raw_df, source_file)

            # ── STEP 4: Write to Delta ────────────────────────────
            self._write_delta(enriched_df, delta_path, table_name)

            # ── STEP 5: Count rows and determine status ──────────
            rows_loaded = self._count_rows(delta_path)
            logger.info(f"[{table_name}] Rows loaded: {rows_loaded:,} | Minimum expected: {min_rows:,}")

            if rows_loaded < min_rows:
                status = "PARTIAL"
                logger.warning(
                    f"[{table_name}] PARTIAL load: {rows_loaded:,} rows loaded but "
                    f"expected at least {min_rows:,}. Possible data loss at source."
                )
            else:
                status = "SUCCESS"
                logger.success(f"[{table_name}] Load SUCCESS ✓")

            # ── STEP 6: Write audit record ────────────────────────
            self.audit_logger.log(
                table_name=table_name,
                source_file=source_file,
                load_status=status,
                rows_loaded=rows_loaded,
                rows_expected_min=min_rows,
                schema_drift_detected=drift_report.to_json() if drift_report.has_drift else None,
            )
            return status

        except Exception as e:
            error_msg = str(e)
            logger.error(f"[{table_name}] FAILED: {error_msg}")
            self.audit_logger.log(
                table_name=table_name,
                source_file=source_file,
                load_status="FAILED",
                error_message=error_msg[:2000],  # Truncate very long stack traces
            )
            return "FAILED"

    # ─────────────────────────────────────────────────────────────
    # PRIVATE METHODS
    # ─────────────────────────────────────────────────────────────

    def _read_csv(self, source_path: str, schema: Optional[StructType]) -> DataFrame:
        """
        Read CSV with schema enforcement where possible.

        We use header=True and inferSchema=False when we have a defined schema.
        This is faster and prevents Spark from doing an extra scan to infer types.
        """
        if not os.path.exists(source_path):
            raise FileNotFoundError(
                f"Source file not found: {source_path}. "
                f"Did you download the Kaggle dataset to data/raw/?"
            )

        reader = (
            self.spark.read
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("multiLine", "true")      # Handle newlines inside quoted fields
            .option("escape", '"')            # Standard CSV escaping
            .option("nullValue", "")          # Treat empty strings as null
        )

        if schema:
            # Use our defined schema — faster, stricter
            df = reader.schema(schema).csv(source_path)
        else:
            # Fallback: let Spark infer (slower, less reliable)
            logger.warning(f"No schema defined for {source_path} — using inferSchema. Define a schema!")
            df = reader.option("inferSchema", "true").csv(source_path)

        return df

    def _add_metadata_columns(self, df: DataFrame, source_file: str) -> DataFrame:
        """
        Inject platform-level metadata columns.
        These are prefixed with _ to distinguish them from source columns.
        """
        # Row hash: MD5 of all source columns concatenated.
        # Used downstream to detect duplicate records (same data, different load).
        source_cols = [F.col(c) for c in df.columns]
        row_hash_expr = F.md5(
            F.concat_ws("|", *[F.coalesce(c.cast("string"), F.lit("NULL")) for c in source_cols])
        )

        enriched = (
            df
            .withColumn("_ingestion_timestamp", F.lit(self.ingestion_timestamp).cast("timestamp"))
            .withColumn("_source_file", F.lit(source_file))
            .withColumn("_pipeline_version", F.lit(pipeline_cfg.version))
            .withColumn("_row_hash", row_hash_expr)
        )
        return enriched

    def _write_delta(self, df: DataFrame, delta_path: str, table_name: str) -> None:
        """
        Write DataFrame to Delta Lake.

        Delta Lake benefits we get automatically:
        - ACID transactions (no partial writes)
        - Time travel (query previous versions with VERSION AS OF)
        - Schema enforcement (Delta rejects schema mismatches on write)
        - Transaction log (full history of all operations)
        """
        os.makedirs(delta_path, exist_ok=True)

        (
            df.write
            .format("delta")
            .mode(self.write_mode)
            .option("overwriteSchema", "true")   # Allow schema evolution on full reload
            .save(delta_path)
        )
        logger.debug(f"[{table_name}] Written to Delta at {delta_path}")

    def _count_rows(self, delta_path: str) -> int:
        """
        Count rows in the just-written Delta table.
        We read back from Delta (not from the input df) to confirm the write succeeded.
        """
        return self.spark.read.format("delta").load(delta_path).count()

    def _get_min_rows(self, table_name: str) -> int:
        """Return minimum expected rows for a table from config."""
        thresholds = {
            table_cfg.orders:      row_thresholds.orders,
            table_cfg.order_items: row_thresholds.order_items,
            table_cfg.payments:    row_thresholds.payments,
            table_cfg.customers:   row_thresholds.customers,
            table_cfg.products:    row_thresholds.products,
            table_cfg.sellers:     row_thresholds.sellers,
            table_cfg.reviews:     row_thresholds.reviews,
            table_cfg.geolocation: row_thresholds.geolocation,
        }
        return thresholds.get(table_name, 0)

    def _log_pipeline_summary(self, results: dict) -> None:
        """Print a clean summary table to logs at the end of the full pipeline run."""
        logger.info("=" * 60)
        logger.info("BRONZE INGESTION PIPELINE — COMPLETE")
        logger.info("=" * 60)
        for table, status in results.items():
            icon = "✓" if status == "SUCCESS" else "⚠" if status == "PARTIAL" else "✗"
            logger.info(f"  {icon}  {table:<40} {status}")
        logger.info("=" * 60)

        failed = [t for t, s in results.items() if s == "FAILED"]
        partial = [t for t, s in results.items() if s == "PARTIAL"]
        if failed:
            logger.error(f"FAILED tables require immediate attention: {failed}")
        if partial:
            logger.warning(f"PARTIAL loads may indicate upstream data loss: {partial}")
