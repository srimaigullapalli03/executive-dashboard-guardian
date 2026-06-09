"""
ingestion/schema_validator.py
------------------------------
Detects schema drift between what we expect and what the source file actually contains.

SCHEMA DRIFT = when a source system changes column names, types, or adds/removes columns
without telling the data team. This is extremely common in real enterprise environments.

Examples of drift we catch:
  - Column renamed: "payment_value" → "payment_amount"
  - Column dropped: "order_estimated_delivery_date" removed from source
  - Column added: new "promo_code" column appears in orders
  - Type changed: price changes from DOUBLE to STRING

We detect drift BEFORE writing to Delta, log it in the audit table,
and decide whether to FAIL the load or WARN and proceed (configurable per table).
"""

import json
from dataclasses import dataclass, field
from typing import List, Optional, Set

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from loguru import logger


@dataclass
class SchemaDriftReport:
    """
    Structured result of a schema comparison.
    Using a dataclass makes it easy to serialize to JSON for the audit log.
    """
    table_name: str
    has_drift: bool
    missing_columns: List[str] = field(default_factory=list)   # Expected but not in source
    extra_columns: List[str] = field(default_factory=list)     # In source but not expected
    type_mismatches: List[str] = field(default_factory=list)   # Column exists but wrong type
    critical_missing: List[str] = field(default_factory=list)  # Missing AND marked not-nullable

    def to_json(self) -> str:
        """Serialise to JSON for storage in the audit log table."""
        return json.dumps({
            "has_drift": self.has_drift,
            "missing_columns": self.missing_columns,
            "extra_columns": self.extra_columns,
            "type_mismatches": self.type_mismatches,
            "critical_missing": self.critical_missing,
        })

    def summary(self) -> str:
        if not self.has_drift:
            return f"[{self.table_name}] Schema OK — no drift detected."
        parts = []
        if self.missing_columns:
            parts.append(f"Missing: {self.missing_columns}")
        if self.extra_columns:
            parts.append(f"Extra: {self.extra_columns}")
        if self.type_mismatches:
            parts.append(f"Type mismatches: {self.type_mismatches}")
        if self.critical_missing:
            parts.append(f"CRITICAL missing (not-nullable): {self.critical_missing}")
        return f"[{self.table_name}] DRIFT DETECTED — " + " | ".join(parts)


def validate_schema(
    df: DataFrame,
    expected_schema: StructType,
    table_name: str,
    critical_columns: Optional[List[str]] = None,
) -> SchemaDriftReport:
    """
    Compare a DataFrame's actual schema against the expected StructType.

    Args:
        df: The ingested DataFrame (before writing to Delta)
        expected_schema: The StructType we defined in schema_definitions.py
        table_name: Used for logging and reporting
        critical_columns: Columns that MUST be present — triggers FAILED status if missing

    Returns:
        SchemaDriftReport with full details of any drift found
    """
    critical_columns = critical_columns or []

    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    expected_fields = {f.name: f.dataType for f in expected_schema.fields}

    actual_names: Set[str] = set(actual_fields.keys())
    expected_names: Set[str] = set(expected_fields.keys())

    missing = sorted(expected_names - actual_names)
    extra = sorted(actual_names - expected_names)

    # Type mismatch check — only for columns present in both
    type_mismatches = []
    for col_name in expected_names & actual_names:
        expected_type = type(expected_fields[col_name]).__name__
        actual_type = type(actual_fields[col_name]).__name__
        if expected_type != actual_type:
            type_mismatches.append(
                f"{col_name}: expected {expected_type}, got {actual_type}"
            )

    critical_missing = [c for c in critical_columns if c in missing]
    has_drift = bool(missing or extra or type_mismatches)

    report = SchemaDriftReport(
        table_name=table_name,
        has_drift=has_drift,
        missing_columns=missing,
        extra_columns=extra,
        type_mismatches=type_mismatches,
        critical_missing=critical_missing,
    )

    if has_drift:
        logger.warning(report.summary())
    else:
        logger.info(report.summary())

    return report
