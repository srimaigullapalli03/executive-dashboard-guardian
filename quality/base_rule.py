"""
quality/base_rule.py
---------------------
Base class that every data quality rule inherits from.

WHY A BASE CLASS?
In production, you might have 50+ data quality rules. Without a base class,
each rule would implement logging, violation recording, and error handling
differently — making the codebase impossible to maintain.

With a base class:
- Every rule has the same interface: rule.run()
- Every rule writes violations in the same format
- Adding a new rule = just inherit and implement _check()
- An interviewer sees this and knows you understand OOP design patterns
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional
import uuid

from pyspark.sql import SparkSession, DataFrame
from loguru import logger


@dataclass
class Violation:
    """
    One data quality problem found.
    Every rule produces a list of these — all written to dq_violations table.
    """
    violation_id: str           # Unique ID for this violation
    rule_name: str              # Which rule caught this (e.g. "DuplicateDetector")
    table_name: str             # Which Bronze table has the problem
    severity: str               # CRITICAL | WARNING | INFO
    description: str            # Human-readable explanation
    affected_rows: int          # How many rows are affected
    sample_values: str          # Example bad values (for debugging)
    check_timestamp: datetime   # When this check ran
    pipeline_run_id: str        # Groups all violations from one pipeline run


class BaseRule(ABC):
    """
    Abstract base class for all data quality rules.

    Every rule MUST implement:
        _check(df, table_name) → List[Violation]

    Every rule INHERITS for free:
        run()        → runs _check() with logging + error handling
        _violation() → helper to create a Violation object cleanly
    """

    def __init__(self, spark: SparkSession, pipeline_run_id: str):
        self.spark = spark
        self.pipeline_run_id = pipeline_run_id
        self.rule_name = self.__class__.__name__

    def run(self, df: DataFrame, table_name: str) -> List[Violation]:
        """
        Public method called by the pipeline.
        Wraps _check() with logging and error handling.
        """
        logger.info(f"[{self.rule_name}] Running on {table_name}...")
        try:
            violations = self._check(df, table_name)
            if violations:
                logger.warning(
                    f"[{self.rule_name}] {table_name} → "
                    f"{len(violations)} violation(s) found"
                )
            else:
                logger.success(f"[{self.rule_name}] {table_name} → PASSED ✓")
            return violations
        except Exception as e:
            logger.error(f"[{self.rule_name}] RULE EXECUTION FAILED on {table_name}: {e}")
            return []

    @abstractmethod
    def _check(self, df: DataFrame, table_name: str) -> List[Violation]:
        """
        Implement this in each subclass.
        Return a list of Violation objects — empty list means no problems found.
        """
        pass

    def _violation(
        self,
        table_name: str,
        severity: str,
        description: str,
        affected_rows: int,
        sample_values: str = "",
    ) -> Violation:
        """Helper to create a Violation with auto-filled fields."""
        return Violation(
            violation_id=str(uuid.uuid4()),
            rule_name=self.rule_name,
            table_name=table_name,
            severity=severity,
            description=description,
            affected_rows=affected_rows,
            sample_values=sample_values[:500],  # Truncate long samples
            check_timestamp=datetime.now(timezone.utc),
            pipeline_run_id=self.pipeline_run_id,
        )
