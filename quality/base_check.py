"""
quality/base_check.py
----------------------
Parent class for all data quality checks.

WHY A BASE CLASS?
All 5 checks follow the exact same pattern:
  1. Read from Bronze Delta table
  2. Run the check logic
  3. Return a result (PASSED / FAILED / WARNING)
  4. Return list of violations found

Instead of repeating that structure 5 times, we define it once here.
Each individual check only needs to implement its own logic.

This is called inheritance — a core OOP concept interviewers love to ask about.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime, timezone
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame
from loguru import logger


@dataclass
class Violation:
    """
    One single data quality problem found.
    Every check produces a list of these.
    """
    check_name: str
    table_name: str
    severity: str          # "CRITICAL" | "WARNING" | "INFO"
    violation_count: int
    violation_detail: str  # Human-readable description
    sample_records: str    # JSON string of first 5 bad records
    check_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class CheckResult:
    """
    The overall result of running one check.
    Contains the status and all violations found.
    """
    check_name: str
    table_name: str
    status: str            # "PASSED" | "FAILED" | "WARNING"
    violations: List[Violation] = field(default_factory=list)
    rows_scanned: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None


class BaseCheck(ABC):
    """
    Abstract base class for all data quality checks.
    Every check must implement the `run()` method.
    """

    def __init__(self, spark: SparkSession, bronze_base_path: str):
        self.spark = spark
        self.bronze_base_path = bronze_base_path
        self.check_name = self.__class__.__name__

    def read_bronze_table(self, table_name: str) -> DataFrame:
        """Read a Bronze Delta table. Used by all checks."""
        path = f"{self.bronze_base_path}/{table_name}"
        logger.info(f"[{self.check_name}] Reading {table_name} from {path}")
        return self.spark.read.format("delta").load(path)

    def records_to_json(self, df: DataFrame, limit: int = 5) -> str:
        """Convert first N rows of a DataFrame to JSON string for violation samples."""
        import json
        rows = df.limit(limit).toJSON().collect()
        return json.dumps(rows)

    @abstractmethod
    def run(self, table_name: str) -> CheckResult:
        """
        Every check must implement this method.
        Takes a table name, returns a CheckResult.
        """
        pass
