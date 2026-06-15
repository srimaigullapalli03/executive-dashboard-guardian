# Executive Dashboard Guardian

A production-grade data quality platform that prevents executives from making decisions using bad data.

## The Problem

Executives rely on dashboards to make critical business decisions. But what happens when the data feeding those dashboards is wrong?

- A pipeline fails silently → revenue appears to drop 90% overnight
- Duplicate orders get loaded → revenue is inflated by $500K
- A critical field goes NULL → average order value shows $0
- Data feed goes stale → executives see yesterday's numbers thinking it's live

**Executive Dashboard Guardian solves this** by validating every piece of data before it reaches the dashboard.

---

## Architecture

```
[ Kaggle Olist CSVs ]
         │
         ▼
┌─────────────────┐
│  BRONZE LAYER   │  Raw ingestion + schema drift detection + audit log
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ DATA QUALITY    │  5 automated checks before data moves forward
│ RULES ENGINE    │  Duplicates, Nulls, Staleness, Volume, Anomalies
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SILVER LAYER   │  Cleaned, typed, trusted data
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  GOLD LAYER     │  Business KPIs and aggregations
└────────┬────────┘
         │
         ▼
[ Power BI Executive Dashboard ]
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.10+ | Pipeline code and validators |
| PySpark 3.5 | Distributed data processing |
| Delta Lake 3.1 | ACID transactions, time travel, schema enforcement |
| Databricks Community Edition | Cloud execution environment |
| Power BI | Executive dashboard layer |
| Loguru | Structured logging |
| pytest | Unit and integration tests |

---

## Dataset

[Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle.

100K+ orders across 8 related tables: orders, order items, payments, customers, products, sellers, reviews, and geolocation.

---

## Project Structure

```
executive_dashboard_guardian/
│
├── config/
│   ├── settings.py              # Centralized config and thresholds
│   └── schema_definitions.py   # Expected schemas for all 8 tables
│
├── ingestion/
│   ├── bronze_loader.py         # Core ingestion engine
│   ├── schema_validator.py      # Schema drift detection
│   └── audit_logger.py          # Ingestion audit log writer
│
├── quality/
│   ├── base_check.py            # Parent class for all checks
│   ├── duplicate_check.py       # Detects duplicate records via MD5 hash
│   ├── null_check.py            # Detects null critical fields
│   ├── staleness_check.py       # Detects stale data feeds
│   ├── volume_check.py          # Detects unexpected row count drops
│   ├── revenue_anomaly_check.py # Detects abnormal revenue via Z-score
│   └── dq_runner.py             # Orchestrates all checks
│
├── silver/
│   ├── silver_orders.py         # Casts timestamps, adds delivery metrics
│   ├── silver_order_items.py    # Adds total item value
│   ├── silver_payments.py       # Standardizes payment types
│   ├── silver_customers.py      # Standardizes city/state
│   ├── silver_products.py       # Cleans category names
│   ├── silver_sellers.py        # Standardizes seller location
│   ├── silver_order_reviews.py  # Adds sentiment classification
│   ├── silver_geolocation.py    # Filters invalid coordinates
│   └── silver_runner.py         # Orchestrates all transformations
│
├── gold/
│   ├── gold_daily_revenue.py         # Revenue per day
│   ├── gold_revenue_by_category.py   # Revenue per product category
│   ├── gold_revenue_by_state.py      # Revenue per Brazilian state
│   ├── gold_customer_satisfaction.py # Review scores by category
│   ├── gold_seller_performance.py    # Revenue and ratings per seller
│   └── gold_runner.py               # Orchestrates all aggregations
│
├── run_pipeline.py           # Run Bronze ingestion
├── run_quality_checks.py     # Run all 5 DQ checks
├── run_silver_pipeline.py    # Run Silver transformations
├── run_gold_pipeline.py      # Run Gold aggregations
└── requirements.txt
```

---

## Data Quality Checks

The platform runs 21 automated checks across all tables before any data reaches the dashboard:

| Check | What It Detects |
|---|---|
| Duplicate Check | Orders loaded more than once — inflates revenue |
| Null Check | Missing critical fields like price or customer ID |
| Staleness Check | Data not refreshed within 24 hours |
| Volume Check | Row count below expected minimum — partial pipeline failure |
| Revenue Anomaly Check | Statistically unusual revenue using Z-score analysis |

---

## Quick Start

### 1. Download Dataset
```bash
pip install kaggle
kaggle datasets download -d olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d data/raw/
```

### 2. Install Dependencies
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the Full Pipeline
```bash
# Step 1: Ingest raw data into Bronze Delta tables
python3 run_pipeline.py

# Step 2: Run all 5 data quality checks
python3 run_quality_checks.py

# Step 3: Clean and transform into Silver tables
python3 run_silver_pipeline.py

# Step 4: Build Gold business aggregations
python3 run_gold_pipeline.py
```

---

## Pipeline Results

```
Bronze Layer   → 8 tables, 1,550,851 records ingested
Quality Checks → 21 checks: 20 passed, 1 revenue anomaly flagged
Silver Layer   → 8 tables, data cleaned and typed
Gold Layer     → 5 business KPI tables ready for Power BI
```

---

## Power BI Dashboard

The Gold layer feeds a Power BI executive dashboard with:

- **Revenue Trend** — Daily revenue from 2016 to 2018
- **Category Breakdown** — Top performing product categories
- **Brazil Revenue Map** — Revenue by state plotted on map
- **Seller Performance** — Top sellers by revenue and rating

---

## Key Design Decisions

**Why Medallion Architecture?**
Bronze preserves raw data as received. If downstream logic has a bug, we replay from Bronze without re-pulling from the source. Clear separation of concerns: ingestion → cleaning → business logic.

**Why MD5 row hashes at Bronze ingestion?**
Hashing at the source gives the purest dedup signal. Hashing after Silver transformations would miss duplicates that arrive in different batches.

**Why Delta Lake over Parquet?**
Delta adds ACID transactions, time travel, schema enforcement, and a transaction log — all on top of Parquet. For a data quality platform, ACID is non-negotiable.

**Why write violations to a Delta table?**
Delta tables are queryable by Power BI. A data quality dashboard showing violation trends over time is far more useful than digging through log files.

---

## Author

Srimai Gullapalli
Data Engineer Portfolio Project
