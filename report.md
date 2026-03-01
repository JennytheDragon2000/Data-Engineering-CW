# CM2606 Data Engineering — Summary Report

---

## Cover Page

| | |
|---|---|
| **Module Code & Name** | CM2606 Data Engineering |
| **Report Title** | End-to-End Sales Analytics Pipeline: Olist Brazilian E-Commerce |
| **Student Name** | *[Your Full Name]* |
| **RGU ID** | *[Your RGU ID]* |
| **IIT Student ID** | *[Your IIT Student ID]* |

---

## 1. Introduction

This report documents the design and implementation of an end-to-end data pipeline built on the Olist Brazilian E-Commerce dataset. Raw transactional CSV files are ingested into a Parquet-based data lake (bronze layer), cleaned and standardised by a distributed PySpark job (silver layer), and finally loaded into a PostgreSQL star-schema data warehouse (gold layer). The complete workflow is orchestrated by a single Apache Airflow DAG, enabling the entire pipeline to run in one execution without manual intervention.

---

## 2. Dataset Selection

The dataset is the **Olist Brazilian E-Commerce** public dataset (Kaggle, CC-BY-NC-SA-4.0), covering approximately 100,000 orders placed on the Olist marketplace between 2016 and 2018. It comprises eight related CSV files spanning customers, orders, order items, payments, reviews, products, sellers, and a Portuguese-to-English product category translation table. The dataset was used in full; no subset was required as PySpark handled the volume without resource issues.

---

## 3. Insight Generation Mechanism

The chosen insight generation mechanism is a **Sales Performance BI Dashboard** to be connected directly to the PostgreSQL data warehouse. The dashboard would specifically enable analysts to answer:

- **Revenue trend analysis** — monthly and quarterly revenue (BRL) over the 2016–2018 period to identify seasonal patterns and growth trajectory.
- **Geographic sales distribution** — revenue and order volume broken down by Brazilian state and city, identifying high- and low-performing regions.
- **Product category performance** — top-selling categories by items sold and revenue, enabling category managers to prioritise stock and promotions.
- **Customer review quality** — average review score per category and seller, surfacing service quality issues.

The star schema is directly designed to serve these queries: `fact_sales` holds the measurable facts (price, freight, payment, review score) and the four dimension tables (date, customer, product, seller) provide the slicing axes. A BI tool such as Apache Superset or Metabase would connect to PostgreSQL via a read-only user and surface these views as interactive charts.

---

## 4. Pipeline Design

```mermaid
flowchart TD
    CFG["config/config.yaml\n(DB credentials · file paths)"]

    subgraph SRC["Data Source"]
        CSV["8 × Olist CSV Files\ndata/raw/"]
    end

    subgraph AF["Apache Airflow — DAG: ecommerce_sales_pipeline  (schedule: @once)"]
        subgraph T1["Task 1 · setup_db  (PythonOperator)"]
            DBSetup["db_setup.py\npsycopg2 DDL"]
        end
        subgraph T2["Task 2 · ingest_data  (PythonOperator)"]
            Ingest["ingestion.py\npandas"]
        end
        subgraph T3["Task 3 · run_etl  (BashOperator · spark-submit)"]
            ETL["etl_spark.py\nPySpark 3.5"]
        end
        T1 --> T2 --> T3
    end

    subgraph LAKE["Data Lake — Medallion Architecture  (local filesystem)"]
        Bronze["Bronze Layer\ndata_lake/bronze/\nRaw Parquet · partitioned by ingestion_date"]
        Silver["Silver Layer\ndata_lake/silver/\nCleaned · Typed · Deduplicated Parquet"]
    end

    subgraph DW["Data Warehouse — PostgreSQL Star Schema"]
        Fact["fact_sales\n(price · freight_value · payment_value · review_score)"]
        DimDate["dim_date"]
        DimCust["dim_customer"]
        DimProd["dim_product"]
        DimSell["dim_seller"]
        Fact -->|date_key FK| DimDate
        Fact -->|customer_key FK| DimCust
        Fact -->|product_key FK| DimProd
        Fact -->|seller_key FK| DimSell
    end

    CSV -->|"pandas read_csv"| Ingest
    Ingest -->|"write Parquet"| Bronze
    Bronze -->|"spark.read.parquet"| ETL
    ETL -->|"write cleaned Parquet"| Silver
    ETL -->|"JDBC · staging-table upsert"| Fact
    DBSetup -->|"CREATE TABLE IF NOT EXISTS"| DW

    CFG -.->|"credentials & paths"| DBSetup
    CFG -.->|"credentials & paths"| Ingest
    CFG -.->|"credentials & paths"| ETL
```

**Components:**

- **Data Source** — Eight CSV files downloaded to `data/raw/` on the local machine.
- **Ingestion Flow** (`ingestion.py`) — pandas reads each CSV and writes it as Parquet to `data_lake/bronze/`, partitioned by `ingestion_date` to support future incremental loads and full audit traceability.
- **Data Lake** — Local filesystem organised into `bronze/` (raw, immutable) and `silver/` (cleaned) layers following the medallion architecture pattern.
- **ETL Flow** (`etl_spark.py`) — A PySpark job reads from bronze, applies four transformations (see Section 5), writes clean data to silver, then loads the PostgreSQL star schema using a staging-table upsert pattern for idempotency.
- **Data Storage** — PostgreSQL star schema with one fact table (`fact_sales`) and four dimension tables (`dim_date`, `dim_customer`, `dim_product`, `dim_seller`). Surrogate keys are managed via `SERIAL` columns; foreign key indexes are created on the fact table for join performance.
- **Orchestration** (`ecommerce_pipeline_dag.py`) — An Airflow DAG (`ecommerce_sales_pipeline`, schedule `@once`) chains three tasks sequentially: `setup_db → ingest_data → run_etl`.

```mermaid
erDiagram
    fact_sales {
        serial      sale_key        PK
        int         date_key        FK
        int         customer_key    FK
        int         product_key     FK
        int         seller_key      FK
        varchar     order_id
        double      price
        double      freight_value
        double      payment_value
        double      review_score
    }
    dim_date {
        serial      date_key    PK
        date        full_date
        smallint    year
        smallint    month
        smallint    quarter
        smallint    day_of_week
    }
    dim_customer {
        serial      customer_key        PK
        varchar     customer_id
        varchar     customer_unique_id
        varchar     customer_city
        char        customer_state
    }
    dim_product {
        serial      product_key             PK
        varchar     product_id
        varchar     product_category_name
        double      product_weight_g
    }
    dim_seller {
        serial      seller_key      PK
        varchar     seller_id
        varchar     seller_city
        char        seller_state
    }

    fact_sales }o--|| dim_date     : "date_key"
    fact_sales }o--|| dim_customer : "customer_key"
    fact_sales }o--|| dim_product  : "product_key"
    fact_sales }o--|| dim_seller   : "seller_key"
```

---

## 5. Technical Implementation

### Tool Comparison

| Component | Option Considered | Option Selected | Justification |
|---|---|---|---|
| Ingestion | PySpark, pandas | **pandas** | CSVs are read once per run; pandas is simpler and sufficient for single-node ingestion without requiring a Spark context |
| Distributed Processing | Apache Flink, Dask, PySpark | **PySpark 3.5** | Industry-standard for batch ETL; native Parquet support; DataFrame API familiar from lecture content |
| Data Lake Format | CSV, Avro, Parquet | **Parquet** | Columnar storage yields faster analytical reads and significant compression versus raw CSV |
| Data Warehouse | MySQL, MongoDB, PostgreSQL | **PostgreSQL** | Mature ACID-compliant RDBMS; strong JDBC support for PySpark; free and open-source |
| Orchestration | Prefect, Luigi, Airflow | **Apache Airflow 2.8** | De-facto industry standard; DAG-based dependency management; built-in logging and retry logic |

### ETL Transformations

Four cleaning steps are implemented in the silver layer:

1. **Deduplication** — `dropDuplicates()` scoped to natural keys (e.g., `order_id`) across all DataFrames.
2. **Missing Value Handling** — `review_score` nulls filled with the median (via `approxQuantile`) to avoid biasing averages; rows missing `order_id` or `customer_id` dropped as they cannot participate in any join.
3. **Data Type Conversion** — Timestamp strings parsed with `to_timestamp()` cast to `TimestampType`; `price`, `freight_value`, and `payment_value` cast to `DoubleType` for correct aggregation.
4. **Data Standardisation** — City and state fields uppercased to eliminate case inconsistencies; product category names translated from Portuguese to English via a left join against the translation lookup table.

### Additional Quality Features

- Ingestion logging records row counts and output file sizes per dataset.
- Upsert logic (`INSERT … ON CONFLICT DO NOTHING`) ensures the pipeline is idempotent and safe to re-run.
- Config-driven design (`config.yaml`) keeps all environment-specific settings (DB credentials, paths) outside the codebase.

---

## 6. Discussion and Conclusion

**Challenges:** The main implementation challenge was loading dimension surrogate keys into the fact table without Spark's native upsert support. This was resolved using a staging-table pattern: PySpark writes the raw fact data to a temporary PostgreSQL table via JDBC, then a psycopg2 connection performs the surrogate key resolution and final insert using a single SQL statement, keeping the logic in the database where joins are most efficient.

**Possible Enhancements:** Replacing the local filesystem data lake with HDFS or cloud object storage (e.g., AWS S3) would enable true distributed reads from multiple Spark workers. Incremental ingestion (processing only new files based on the `ingestion_date` partition) would reduce runtime as the dataset grows. Adding a data quality framework such as Great Expectations would formalise schema validation and row-count checks as pipeline gates. Finally, connecting Apache Superset to the PostgreSQL warehouse would complete the insight generation layer envisaged in the design.
