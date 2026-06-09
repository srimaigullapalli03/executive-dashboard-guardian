"""
ingestion/audit_logger.py
--------------------------
Writes every ingestion attempt — success or failure — to a Delta audit log table.
"""

import uuid
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)
from loguru import logger

from config.settings import pipeline_cfg, path_cfg, table_cfg


# Explicit schema — fixes the CANNOT_DETERMINE_TYPE error
AUDIT_SCHEMA = StructType([
    StructField("audit_id",               StringType(),    nullable=False),
    StructField("pipeline_name",          StringType(),    nullable=True),
    StructField("pipeline_version",       StringType(),    nullable=True),
    StructField("table_name",             StringType(),    nullable=False),
    StructField("source_file",            StringType(),    nullable=True),
    StructField("load_status",            StringType(),    nullable=False),
    StructField("rows_loaded",            LongType(),      nullable=True),
    StructField("rows_expected_min",      LongType(),      nullable=True),
    StructField("schema_drift_detected",  StringType(),    nullable=True),
    StructField("error_message",          StringType(),    nullable=True),
    StructField("ingestion_timestamp",    TimestampType(), nullable=False),
    StructField("environment",            StringType(),    nullable=True),
])


class AuditLogger:
    """
    Appends a single audit record to the bronze_ingestion_audit Delta table.
    Uses append mode — we NEVER overwrite audit history.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.audit_table_path = f"{path_cfg.audit_path}/{table_cfg.audit_log}"

    def log(
        self,
        table_name: str,
        source_file: str,
        load_status: str,
        rows_loaded: Optional[int] = None,
        rows_expected_min: Optional[int] = None,
        schema_drift_detected: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Write one audit record per table per pipeline run."""

        log_level = "info" if load_status == "SUCCESS" else "warning" if load_status == "PARTIAL" else "error"
        getattr(logger, log_level)(
            f"[AUDIT] {table_name} | {load_status} | "
            f"rows_loaded={rows_loaded} | rows_expected_min={rows_expected_min}"
        )

        try:
            audit_record = [(
                str(uuid.uuid4()),
                pipeline_cfg.pipeline_name,
                pipeline_cfg.version,
                table_name,
                source_file,
                load_status,
                int(rows_loaded) if rows_loaded is not None else None,
                int(rows_expected_min) if rows_expected_min is not None else None,
                schema_drift_detected,
                error_message[:2000] if error_message else None,
                datetime.now(timezone.utc),
                pipeline_cfg.env,
            )]

            audit_df = self.spark.createDataFrame(audit_record, schema=AUDIT_SCHEMA)

            (
                audit_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(self.audit_table_path)
            )
            logger.info(f"[AUDIT] Record written successfully for {table_name}")

        except Exception as e:
            logger.critical(
                f"AUDIT LOG WRITE FAILED for {table_name}: {e}. "
                f"Original status was: {load_status}"
            )

    def get_last_load_status(self, table_name: str) -> Optional[str]:
        """Fetch the most recent load status for a table."""
        try:
            df = self.spark.read.format("delta").load(self.audit_table_path)
            result = (
                df.filter(df.table_name == table_name)
                .orderBy(df.ingestion_timestamp.desc())
                .limit(1)
                .select("load_status")
                .collect()
            )
            return result[0]["load_status"] if result else None
        except Exception:
            return None
