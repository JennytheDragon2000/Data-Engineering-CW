# Olist E-Commerce Sales Pipeline

CM2606 Data Engineering Coursework — end-to-end pipeline: CSV ingestion → Parquet data lake → PostgreSQL star-schema data warehouse, orchestrated by Apache Airflow.

---

## Architecture

```
data/raw/ (CSVs)
    ↓ ingestion.py (pandas)
data_lake/bronze/ (Parquet, partitioned by ingestion_date)
    ↓ etl_spark.py — silver layer (PySpark cleaning)
data_lake/silver/ (Parquet, cleaned)
    ↓ etl_spark.py — gold layer (DW load)
PostgreSQL — star schema (dim_* + fact_sales)
```

Airflow DAG `ecommerce_sales_pipeline` chains all three steps.

---

## Star Schema

| Table | Description |
|-------|-------------|
| `fact_sales` | Order-item grain: price, freight_value, payment_value, review_score |
| `dim_date` | Calendar attributes derived from order purchase timestamps |
| `dim_customer` | Customer city / state |
| `dim_product` | Product category (English), weight |
| `dim_seller` | Seller city / state |

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Python | ≥ 3.10 |
| Java (for Spark) | 11 or 17 |
| Apache Spark | 3.5.x |
| PostgreSQL | ≥ 14 |
| Apache Airflow | 2.8.x |

---

## Setup

### 1. Clone / download the project

```bash
cd /path/to/ecommerce-pipeline
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Download the dataset

Download the **Olist Brazilian E-Commerce** dataset from Kaggle and place all CSV files in `data/raw/`:

```
data/raw/
├── olist_customers_dataset.csv
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
└── product_category_name_translation.csv
```

### 4. Download the PostgreSQL JDBC driver

Place `postgresql-42.7.3.jar` in `jars/`:

```bash
mkdir -p jars
curl -L -o jars/postgresql-42.7.3.jar \
  https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

### 5. Create the PostgreSQL database

```sql
CREATE DATABASE ecommerce_dw;
```

### 6. Edit `config/config.yaml`

Update `database.user`, `database.password` (and `host`/`port` if not localhost).

---

## Running Manually (step-by-step)

```bash
# Step 1 — Create star schema tables
python scripts/db_setup.py

# Step 2 — Ingest CSVs to bronze Parquet layer
python scripts/ingestion.py

# Step 3 — PySpark ETL: bronze → silver → DW
spark-submit --jars jars/postgresql-42.7.3.jar scripts/etl_spark.py
```

---

## Running via Airflow

### 1. Initialise Airflow (first time only)

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
airflow users create \
  --username admin --password admin \
  --firstname A --lastname B \
  --role Admin --email admin@example.com
```

### 2. Point Airflow at the DAG folder

In `airflow_home/airflow.cfg` set:

```ini
dags_folder = /absolute/path/to/ecommerce-pipeline/dags
```

### 3. Start Airflow services

```bash
airflow webserver --port 8080 &
airflow scheduler &
```

### 4. Trigger the DAG

Open `http://localhost:8080`, find `ecommerce_sales_pipeline`, and click **Trigger DAG**.

All three tasks (`setup_db → ingest_data → run_etl`) should turn green.

---

## Verify Results

```sql
-- Connect to ecommerce_dw
\c ecommerce_dw

SELECT COUNT(*) FROM fact_sales;      -- ~112 000 rows
SELECT COUNT(*) FROM dim_customer;    -- ~99 000 rows
SELECT COUNT(*) FROM dim_product;     -- ~33 000 rows
SELECT COUNT(*) FROM dim_seller;      -- ~3 000 rows
SELECT COUNT(*) FROM dim_date;        -- ~700 distinct dates

-- Sample BI query: monthly revenue
SELECT d.year, d.month, ROUND(SUM(f.payment_value)::numeric, 2) AS revenue
FROM fact_sales f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

---

## ETL Transformations Implemented

1. **Deduplication** — `dropDuplicates()` on all DataFrames before writing to silver
2. **Missing Value Handling** — `review_score` nulls filled with median; rows missing `order_id` / `customer_id` dropped
3. **Data Type Casting** — timestamps parsed with `to_timestamp()`; numeric columns cast to `DoubleType`
4. **Standardisation** — city/state uppercased; product category names translated to English via lookup table
