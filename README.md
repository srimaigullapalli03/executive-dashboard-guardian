# Executive Dashboard Guardian
### A production-grade data quality platform for executive analytics

> *"Bad data is worse than no data. No data gets you a question mark. Bad data gets you a wrong decision."*

---

## What This Project Does

Executive Dashboard Guardian is a data platform that **prevents executives from making decisions using bad data**. Before any number reaches a Power BI dashboard, it must pass through a multi-layer validation pipeline that detects:

| Problem | Detection Method |
|---|---|
| Missing sales transactions | Row count threshold monitoring |
| Duplicate records | MD5 row hash deduplication |
| Stale data feeds | Ingestion timestamp staleness checks |
| Null critical fields | Not-null validation on key columns |
| Abnormal revenue changes | Statistical anomaly detection (Z-score + IQR) |

---

## Architecture: Medallion Pattern

```
[ Kaggle Olist CSVs ]
         │
         ▼
 ┌───────────────┐
 │  BRONZE LAYER │  ← Raw data + metadata + audit log
 │  (Phase 1)    │  ← Schema drift detection
 └───────┬───────┘
         │
         ▼
 ┌───────────────┐
 │  SILVER LAYER │  ← Cleaned, typed, deduplicated
 │  (Phase 2)    │  ← Data quality rules applied
 └───────┬───────┘
         │
         ▼
 ┌───────────────┐
 │  GOLD LAYER   │  ← Business aggregates, KPIs
 │  (Phase 3)    │  ← Dashboard-ready
 └───────┬───────┘
         │
         ▼
 [ Power BI Dashboard ]  ← Only reaches here if ALL checks pass
```

---

## Tech Stack

- **Python 3.10+** — Pipeline code, validators, tests
- **PySpark 3.5 / Delta Lake 3.1** — Distributed processing, ACID tables
- **Databricks Community Edition** — Cloud execution environment
- **Delta Lake** — Time travel, schema enforcement, ACID transactions
- **Power BI** — Executive dashboard layer
- **Loguru** — Structured logging
- **pytest** — Unit and integration tests
- **Great Expectations** *(Phase 3)* — Advanced data validation

---

## Dataset

[Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — Kaggle

~100K orders, 5 related tables: orders, order items, payments, customers, products.

---

## Project Phases

| Phase | Name | Status |
|---|---|---|
| 1 | Bronze Layer Ingestion | ✅ Complete |
| 2 | Data Quality Rules | 🔄 Next |
| 3 | Silver Layer Transformation | ⏳ Planned |
| 4 | Gold Layer Aggregation | ⏳ Planned |
| 5 | Anomaly Detection Engine | ⏳ Planned |
| 6 | Power BI Integration | ⏳ Planned |
| 7 | Alerting & Observability | ⏳ Planned |

---

## Phase 1: Quick Start

### 1. Download Dataset
```bash
# Install Kaggle CLI
pip install kaggle

# Download Olist dataset
kaggle datasets download -d olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d data/raw/
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment
```bash
cp .env.example .env
# Edit .env with your paths
```

### 4. Run Ingestion (Local)
```python
from pyspark.sql import SparkSession
from ingestion.bronze_loader import BronzeLoader

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

loader = BronzeLoader(spark)
results = loader.run_full_pipeline()
```

### 5. Run on Databricks Community Edition
1. Upload CSVs to DBFS: `File > Upload Data`
2. Import `notebooks/01_bronze_ingestion.py`
3. Attach to a cluster with DBR 13.x+
4. Run All Cells

### 6. Run Tests
```bash
pytest tests/ -v --cov=ingestion --cov-report=term-missing
```

---

## What Gets Logged

Every pipeline run writes to the `bronze_ingestion_audit` Delta table:

```sql
SELECT
    table_name,
    load_status,
    rows_loaded,
    rows_expected_min,
    schema_drift_detected,
    ingestion_timestamp
FROM bronze_ingestion_audit
ORDER BY ingestion_timestamp DESC;
```

---

## Key Design Decisions

**Why Bronze = raw?**
Bronze tables preserve exactly what the source sent. If a downstream bug corrupts Silver or Gold data, we always have the original Bronze to replay from. Never modify Bronze records.

**Why MD5 row hashes at ingestion?**
Hashing at the point of ingestion (before any transformation) means our dedup logic in Phase 2 can catch duplicates that arrive in different batches — not just within a single load.

**Why schema validation before writing?**
Schema drift discovered after 3 months of incorrect data is a production incident. Schema drift discovered at ingestion is a config update.

**Why a separate audit log table (not just application logs)?**
Application logs are for engineers. The audit Delta table is queryable by SQL, can be connected to Power BI for pipeline health dashboards, can be queried by data quality checks, and persists across deployments. It's the operational backbone of the platform.
