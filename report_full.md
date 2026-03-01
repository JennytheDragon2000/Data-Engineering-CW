# CM2606 Data Engineering Coursework

## End-to-End Sales Analytics Pipeline — Olist Brazilian E-Commerce

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Dataset Overview](#2-dataset-overview)
3. [System Architecture](#3-system-architecture)
4. [Data Lake Design](#4-data-lake-design)
5. [Data Warehouse Design](#5-data-warehouse-design)
6. [ETL Transformations](#6-etl-transformations)
7. [Orchestration with Apache Airflow](#7-orchestration-with-apache-airflow)
8. [Pipeline Execution and Results](#8-pipeline-execution-and-results)
9. [Conclusion](#9-conclusion)

---

## 1. Introduction

This report documents the design and implementation of an end-to-end data engineering pipeline for the CM2606 coursework. The pipeline ingests raw transactional data from the Olist Brazilian E-Commerce dataset, processes it through a multi-layer data lake, and loads a star-schema data warehouse in PostgreSQL. The complete workflow is orchestrated using Apache Airflow.

**Use case:** A Sales Performance BI Dashboard that enables analysts to query revenue, order volumes, and customer behaviour across time, geography, and product category dimensions.

**Technology stack:**

| Component      | Technology                 |
| -------------- | -------------------------- |
| Ingestion      | Python 3.10, pandas 2.1    |
| Processing     | Apache Spark 3.5 (PySpark) |
| Storage (lake) | Apache Parquet             |
| Data warehouse | PostgreSQL 14              |
| Orchestration  | Apache Airflow 2.8         |

---

## 2. Dataset Overview

The dataset is sourced from Kaggle ([Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)) and covers 100,000 orders placed on the Olist marketplace between 2016 and 2018.

### 2.1 Source Files

| File                                    | Rows    | Key Columns                                                            |
| --------------------------------------- | ------- | ---------------------------------------------------------------------- |
| `olist_orders_dataset.csv`              | 99,441  | `order_id`, `customer_id`, `order_purchase_timestamp`, `order_status`  |
| `olist_order_items_dataset.csv`         | 112,650 | `order_id`, `product_id`, `seller_id`, `price`, `freight_value`        |
| `olist_order_payments_dataset.csv`      | 103,886 | `order_id`, `payment_type`, `payment_value`                            |
| `olist_order_reviews_dataset.csv`       | 104,719 | `order_id`, `review_score`                                             |
| `olist_customers_dataset.csv`           | 99,441  | `customer_id`, `customer_unique_id`, `customer_city`, `customer_state` |
| `olist_products_dataset.csv`            | 32,951  | `product_id`, `product_category_name`, `product_weight_g`              |
| `olist_sellers_dataset.csv`             | 3,095   | `seller_id`, `seller_city`, `seller_state`                             |
| `product_category_name_translation.csv` | 71      | `product_category_name`, `product_category_name_english`               |

### 2.2 Entity Relationship (Source)

<!-- IMAGE PLACEHOLDER: ER diagram of the 8 source CSV files showing join keys between orders, customers, items, products, sellers, payments, and reviews. Export from draw.io or dbdiagram.io. Suggested filename: img/source_erd.png -->

![Source Entity Relationship Diagram](img/source_erd.png)

---

## 3. System Architecture

The pipeline follows a **medallion architecture** (Bronze → Silver → Gold), with each layer increasing in data quality and analytical readiness.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Apache Airflow DAG                          │
│  [setup_db] ──────────► [ingest_data] ──────────► [run_etl]         │
└─────────────────────────────────────────────────────────────────────┘
        │                       │                        │
        ▼                       ▼                        ▼
  PostgreSQL             data/raw/ (CSV)          data_lake/bronze/
  (schema DDL)      ──► pandas read_csv      ──►  (Parquet, partitioned
                         + write Parquet           by ingestion_date)
                                                        │
                                                        ▼
                                                 data_lake/silver/
                                                 (Parquet, cleaned)
                                                        │
                                                        ▼
                                                  PostgreSQL DW
                                                  (star schema)
```

<!-- IMAGE PLACEHOLDER: Architecture diagram showing the three pipeline stages and data flow between components. A clean flow diagram with colour-coded layers (bronze/silver/gold) works well. Suggested filename: img/architecture.png -->

![Pipeline Architecture Diagram](img/architecture.png)

### 3.1 Project Structure

```
ecommerce-pipeline/
├── config/
│   └── config.yaml              # Central config: DB, paths, dataset list
├── data/
│   └── raw/                     # Source CSVs (downloaded from Kaggle)
├── data_lake/
│   ├── bronze/                  # Raw Parquet, partitioned by ingestion_date
│   └── silver/                  # Cleaned Parquet
├── scripts/
│   ├── db_setup.py              # Creates PostgreSQL star schema tables
│   ├── ingestion.py             # CSV → Parquet (bronze layer)
│   └── etl_spark.py             # PySpark: bronze → silver → PostgreSQL
├── dags/
│   └── ecommerce_pipeline_dag.py
├── requirements.txt
└── README.md
```

---

## 4. Data Lake Design

### 4.1 Bronze Layer — Raw Ingestion

`scripts/ingestion.py` reads each CSV with `pandas` and writes it to Parquet using `pyarrow`. Files are **partitioned by ingestion date** to support future incremental loads and allow audit tracing:

```
data_lake/bronze/
└── olist_orders_dataset/
    └── ingestion_date=2026-03-01/
        └── olist_orders_dataset.parquet
```

Each file also gains an `_ingestion_date` metadata column. Ingestion counts and file sizes are logged to stdout for observability.

### 4.2 Silver Layer — Cleaned Data

`scripts/etl_spark.py` reads from bronze and applies the four transformations described in [Section 6](#6-etl-transformations). Cleaned DataFrames are written back to `data_lake/silver/<table_name>/` in overwrite mode.

<!-- IMAGE PLACEHOLDER: Screenshot of the data_lake/ directory tree in a file explorer or terminal, showing the partitioned bronze folders and silver output folders. Suggested filename: img/datalake_tree.png -->

![Data Lake Directory Structure](img/datalake_tree.png)

---

## 5. Data Warehouse Design

### 5.1 Star Schema

The data warehouse uses a **star schema** optimised for analytical queries. A single fact table at order-item grain is surrounded by four dimension tables.

<!-- IMAGE PLACEHOLDER: Star schema ERD showing fact_sales at the centre connected to dim_date, dim_customer, dim_product, and dim_seller with labelled FK arrows and column lists. Generate from pgAdmin's ERD tool or dbdiagram.io. Suggested filename: img/star_schema.png -->

![Star Schema Diagram](img/star_schema.png)

### 5.2 Table Definitions

#### `fact_sales`

| Column          | Type        | Description                      |
| --------------- | ----------- | -------------------------------- |
| `order_item_sk` | SERIAL PK   | Surrogate key                    |
| `date_key`      | INT FK      | → `dim_date`                     |
| `customer_key`  | INT FK      | → `dim_customer`                 |
| `product_key`   | INT FK      | → `dim_product`                  |
| `seller_key`    | INT FK      | → `dim_seller`                   |
| `order_id`      | VARCHAR(64) | Source order reference           |
| `price`         | DOUBLE      | Item price                       |
| `freight_value` | DOUBLE      | Freight cost                     |
| `payment_value` | DOUBLE      | Total payment (summed per order) |
| `review_score`  | DOUBLE      | Average review score (per order) |

#### `dim_date`

| Column        | Type        | Description                   |
| ------------- | ----------- | ----------------------------- |
| `date_key`    | SERIAL PK   | Surrogate key                 |
| `full_date`   | DATE UNIQUE | Calendar date                 |
| `year`        | SMALLINT    |                               |
| `month`       | SMALLINT    | 1–12                          |
| `quarter`     | SMALLINT    | 1–4                           |
| `day_of_week` | SMALLINT    | 1 = Sunday (Spark convention) |

#### `dim_customer`

| Column               | Type               |
| -------------------- | ------------------ |
| `customer_key`       | SERIAL PK          |
| `customer_id`        | VARCHAR(64) UNIQUE |
| `customer_unique_id` | VARCHAR(64)        |
| `customer_city`      | VARCHAR(128)       |
| `customer_state`     | CHAR(2)            |

#### `dim_product`

| Column                  | Type               |
| ----------------------- | ------------------ |
| `product_key`           | SERIAL PK          |
| `product_id`            | VARCHAR(64) UNIQUE |
| `product_category_name` | VARCHAR(128)       |
| `product_weight_g`      | DOUBLE             |

#### `dim_seller`

| Column         | Type               |
| -------------- | ------------------ |
| `seller_key`   | SERIAL PK          |
| `seller_id`    | VARCHAR(64) UNIQUE |
| `seller_city`  | VARCHAR(128)       |
| `seller_state` | CHAR(2)            |

### 5.3 Indexes

Foreign key indexes are created on `fact_sales` to accelerate dimension joins:

```sql
CREATE INDEX idx_fact_sales_date     ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product  ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_seller   ON fact_sales(seller_key);
```

---

## 6. ETL Transformations

Four transformations are applied in the silver layer by `scripts/etl_spark.py`.

### 6.1 Deduplication

`dropDuplicates()` is applied to every DataFrame after reading from bronze. For tables with a natural key (e.g., `order_id`, `customer_id`, `product_id`), deduplication is scoped to that key to preserve the first occurrence.

```python
df = df.dropDuplicates(["order_id"])
```

**Rationale:** Duplicate records in transactional systems are common due to retry logic or ETL re-runs. Failing to remove them would inflate revenue and order-count metrics.

### 6.2 Missing Value Handling

- **`review_score`** — nulls are filled with the **median** score (computed via `approxQuantile`) to avoid biasing the average review metric downward.
- **Critical join keys** (`order_id`, `customer_id`) — rows missing these values are dropped because they cannot be joined to any dimension or used in the fact table.

```python
median_score = df.approxQuantile("review_score", [0.5], 0.01)[0]
df = df.fillna({"review_score": median_score})

df = df.dropna(subset=["order_id", "customer_id"])
```

**Rationale:** Using the median for imputation is more robust than the mean in the presence of skewed ratings distributions (common in review data).

### 6.3 Data Type Casting

All timestamp strings are parsed to `TimestampType` and numeric columns are cast to `DoubleType`:

```python
df = df.withColumn("order_purchase_timestamp",
    F.to_timestamp("order_purchase_timestamp").cast(TimestampType()))

df = df.withColumn("price",         F.col("price").cast(DoubleType()))
df = df.withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
df = df.withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
```

**Rationale:** CSV files store all values as strings. Explicit casting ensures Spark uses efficient columnar storage in Parquet and prevents silent precision loss or incorrect aggregation.

### 6.4 Data Standardisation

- **City and state fields** (`customer_city`, `customer_state`, `seller_city`, `seller_state`) are uppercased to eliminate case inconsistencies (e.g., `São Paulo` vs `SAO PAULO`).
- **Product category names** are translated from Portuguese to English via a left join against the `product_category_name_translation` lookup table.

```python
df = df.withColumn("customer_city", F.upper(F.col("customer_city")))

df = df.join(translations, df["product_category_name"] ==
             translations["product_category_name"], "left")
```

**Rationale:** Inconsistent string casing would cause GROUP BY queries to produce multiple rows for the same city/state. Translating categories makes the dashboard accessible to English-speaking analysts.

---

## 7. Orchestration with Apache Airflow

### 7.1 DAG Design

The Airflow DAG `ecommerce_sales_pipeline` chains the three pipeline steps sequentially using a combination of `PythonOperator` and `BashOperator`.

```
setup_db  ──►  ingest_data  ──►  run_etl
```

| Task          | Operator       | Script                              |
| ------------- | -------------- | ----------------------------------- |
| `setup_db`    | PythonOperator | `scripts/db_setup.py`               |
| `ingest_data` | PythonOperator | `scripts/ingestion.py`              |
| `run_etl`     | BashOperator   | `spark-submit scripts/etl_spark.py` |

**Schedule:** `@once` — the DAG is designed for a single manual trigger, as appropriate for a batch load of a static historical dataset.

### 7.2 DAG Graph View

<!-- IMAGE PLACEHOLDER: Screenshot of the Airflow UI showing the DAG graph view with all three tasks in the "success" (green) state after a completed run. Capture from http://localhost:8080. Suggested filename: img/airflow_dag_success.png -->

![Airflow DAG — Successful Run](img/airflow_dag_success.png)

### 7.3 Task Logs

<!-- IMAGE PLACEHOLDER: Screenshot of the Airflow task log for the run_etl task, showing Spark output lines confirming silver layer row counts and "ETL complete." message. Suggested filename: img/airflow_etl_log.png -->

![Airflow — ETL Task Log](img/airflow_etl_log.png)

---

## 8. Pipeline Execution and Results

### 8.1 Running the Pipeline

Each script can be run independently for testing:

```bash
# Step 1 — Create star schema
python scripts/db_setup.py

# Step 2 — Ingest CSVs to bronze
python scripts/ingestion.py

# Step 3 — PySpark ETL
spark-submit --jars jars/postgresql-42.7.3.jar scripts/etl_spark.py
```

Or trigger the full pipeline via the Airflow UI.

### 8.2 Row Counts After Load

| Table          | Expected Rows |
| -------------- | ------------- |
| `fact_sales`   | ~112,000      |
| `dim_customer` | ~99,400       |
| `dim_product`  | ~32,900       |
| `dim_seller`   | ~3,000        |
| `dim_date`     | ~700          |

<!-- IMAGE PLACEHOLDER: Screenshot of a psql or pgAdmin query window showing SELECT COUNT(*) results for each of the five tables. Suggested filename: img/row_counts.png -->

![PostgreSQL Row Counts](img/row_counts.png)

### 8.3 Sample BI Query — Monthly Revenue

The following query demonstrates the warehouse is ready for analytical use:

```sql
SELECT
    d.year,
    d.month,
    COUNT(DISTINCT f.order_id)          AS orders,
    ROUND(SUM(f.payment_value)::numeric, 2) AS revenue_brl
FROM fact_sales f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

<!-- IMAGE PLACEHOLDER: Screenshot of the query result in psql or pgAdmin showing monthly revenue figures from 2016–2018, or alternatively a bar chart generated from these results. Suggested filename: img/monthly_revenue.png -->

![Monthly Revenue Query Result](img/monthly_revenue.png)

### 8.4 Sample BI Query — Top Product Categories by Revenue

```sql
SELECT
    p.product_category_name,
    ROUND(SUM(f.price)::numeric, 2) AS total_revenue_brl,
    COUNT(*)                        AS items_sold
FROM fact_sales f
JOIN dim_product p ON p.product_key = f.product_key
WHERE p.product_category_name IS NOT NULL
GROUP BY p.product_category_name
ORDER BY total_revenue_brl DESC
LIMIT 10;
```

<!-- IMAGE PLACEHOLDER: Screenshot or table of the top 10 product categories by revenue. Suggested filename: img/top_categories.png -->

![Top Product Categories](img/top_categories.png)

---

## 9. Conclusion

This project successfully delivers an end-to-end data pipeline meeting all coursework requirements:

- **Ingestion** — raw CSVs are reliably loaded into a partitioned Parquet bronze layer with logging and metadata.
- **Processing** — a PySpark job implements four documented transformations (deduplication, null handling, type casting, standardisation) and produces a clean silver layer.
- **Data Warehouse** — a star schema in PostgreSQL is populated via idempotent upsert logic, supporting re-runs without data duplication.
- **Orchestration** — an Apache Airflow DAG with `@once` scheduling encapsulates the full pipeline as three sequentially dependent tasks.

The resulting data warehouse is ready to support a BI dashboard tool (e.g., Metabase, Apache Superset, or Tableau) connecting directly to PostgreSQL for sales performance reporting.
