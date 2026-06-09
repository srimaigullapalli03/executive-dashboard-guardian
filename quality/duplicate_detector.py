"""
quality/duplicate_detector.py
------------------------------
Rule 1: Detect duplicate records in Bronze tables.

HOW IT WORKS:
Every row in Bronze has a _row_hash column — an MD5 fingerprint of all its values.
If two rows have the SAME hash, they are 100% identical records.
This happens when:
  - A source system sends the same file twice
  - An ETL job runs twice due to a retry
  - A payment gateway sends duplicate transaction confirmations

WHY THIS MATTERS FOR DASHBOARDS:
Duplicate orders = inflated revenue numbers.
Example: 1,000 real orders + 1,000 duplicates = dashboard shows 2x revenue.
An executive sees "best month ever!" — finance team discovers the truth later.

TWO TYPES OF DUPLICATES WE CHECK:
1. Hash duplicates  → completely identical rows (same _row_hash)
2. Key duplicates   → same business key (e.g. order_id) appears multiple times
                      but with slightly different values (e.g. different timestamps)
"""

from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from loguru import logger

from quality.base_rule import BaseRule, Violation


# Business key per table — the column that should be unique
# order_id should appear once in orders, but CAN appear multiple times
# in order_items (one order can have many items)
BUSINESS_KEYS = {
    "bronze_orders":         ["order_id"],
    "bronze_order_payments": ["order_id", "payment_sequential"],
    "bronze_customers":      ["customer_id"],
    "bronze_products":       ["product_id"],
    "bronze_sellers":        ["seller_id"],
    "bronze_order_reviews":  ["review_id"],
    # order_items and geolocation intentionally excluded —
    # they naturally have multiple rows per key
}


class DuplicateDetector(BaseRule):
    """
    Detects two kinds of duplicates:
    1. Exact duplicates    — same _row_hash appears more than once
    2. Business key dupes  — same primary key with different values
    """

    def _check(self, df: DataFrame, table_name: str) -> List[Violation]:
        violations = []

        # ── CHECK 1: Exact row duplicates via _row_hash ───────────
        violations += self._check_hash_duplicates(df, table_name)

        # ── CHECK 2: Business key duplicates ─────────────────────
        business_keys = BUSINESS_KEYS.get(table_name)
        if business_keys:
            violations += self._check_key_duplicates(df, table_name, business_keys)

        return violations

    def _check_hash_duplicates(self, df: DataFrame, table_name: str) -> List[Violation]:
        """Find rows where _row_hash appears more than once."""
        if "_row_hash" not in df.columns:
            logger.warning(f"[{table_name}] No _row_hash column found — skipping hash dedup check")
            return []

        # Count how many times each hash appears
        hash_counts = (
            df.groupBy("_row_hash")
            .count()
            .filter(F.col("count") > 1)  # Only keep hashes that appear MORE than once
        )

        duplicate_count = hash_counts.count()

        if duplicate_count == 0:
            return []

        # Count total affected rows
        total_affected = (
            hash_counts
            .withColumn("extra_copies", F.col("count") - 1)
            .agg(F.sum("extra_copies"))
            .collect()[0][0] or 0
        )

        # Get sample duplicate hashes for debugging
        sample = [
            row["_row_hash"][:16] + "..."
            for row in hash_counts.limit(3).collect()
        ]

        return [self._violation(
            table_name=table_name,
            severity="CRITICAL",
            description=(
                f"Found {duplicate_count:,} duplicate row hashes affecting "
                f"{total_affected:,} extra rows. These are 100% identical records "
                f"that will inflate aggregations."
            ),
            affected_rows=int(total_affected),
            sample_values=f"Sample duplicate hashes: {sample}",
        )]

    def _check_key_duplicates(
        self, df: DataFrame, table_name: str, key_columns: List[str]
    ) -> List[Violation]:
        """
        Find rows where the business key appears more than once
        but with different content (different hash = different values).
        This catches updated records sent without deletes,
        or the same order appearing with different statuses.
        """
        # Check all key columns exist in this DataFrame
        missing_keys = [k for k in key_columns if k not in df.columns]
        if missing_keys:
            logger.warning(f"[{table_name}] Key columns {missing_keys} not found — skipping key dedup")
            return []

        key_dupes = (
            df.groupBy(*key_columns)
            .agg(
                F.count("*").alias("row_count"),
                F.countDistinct("_row_hash").alias("distinct_hashes"),
            )
            # More than 1 row AND more than 1 distinct hash = same key, different values
            .filter((F.col("row_count") > 1) & (F.col("distinct_hashes") > 1))
        )

        dupe_key_count = key_dupes.count()

        if dupe_key_count == 0:
            return []

        # Get sample duplicate keys
        sample_rows = key_dupes.limit(3).collect()
        sample = [
            {k: row[k] for k in key_columns}
            for row in sample_rows
        ]

        return [self._violation(
            table_name=table_name,
            severity="WARNING",
            description=(
                f"Found {dupe_key_count:,} business keys {key_columns} that appear "
                f"multiple times with DIFFERENT values. This may indicate upserts "
                f"that weren't deduplicated before landing in Bronze."
            ),
            affected_rows=dupe_key_count,
            sample_values=f"Sample duplicate keys: {sample}",
        )]
