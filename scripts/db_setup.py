"""
db_setup.py — Creates the star schema tables in PostgreSQL.
Run once before the pipeline: python scripts/db_setup.py
"""

import sys
import os
import yaml
import psycopg2
from psycopg2 import sql

# Allow imports relative to project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


def load_config():
    config_path = os.path.join(PROJECT_ROOT, "config", "config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


DDL_STATEMENTS = [
    # Dimension: Date
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key        SERIAL PRIMARY KEY,
        full_date       DATE        NOT NULL UNIQUE,
        year            SMALLINT    NOT NULL,
        month           SMALLINT    NOT NULL,
        quarter         SMALLINT    NOT NULL,
        day_of_week     SMALLINT    NOT NULL
    );
    """,

    # Dimension: Customer
    """
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_key        SERIAL PRIMARY KEY,
        customer_id         VARCHAR(64) NOT NULL UNIQUE,
        customer_unique_id  VARCHAR(64),
        customer_city       VARCHAR(128),
        customer_state      CHAR(2)
    );
    """,

    # Dimension: Product
    """
    CREATE TABLE IF NOT EXISTS dim_product (
        product_key             SERIAL PRIMARY KEY,
        product_id              VARCHAR(64) NOT NULL UNIQUE,
        product_category_name   VARCHAR(128),
        product_weight_g        DOUBLE PRECISION
    );
    """,

    # Dimension: Seller
    """
    CREATE TABLE IF NOT EXISTS dim_seller (
        seller_key      SERIAL PRIMARY KEY,
        seller_id       VARCHAR(64) NOT NULL UNIQUE,
        seller_city     VARCHAR(128),
        seller_state    CHAR(2)
    );
    """,

    # Fact: Sales
    """
    CREATE TABLE IF NOT EXISTS fact_sales (
        order_item_sk   SERIAL PRIMARY KEY,
        date_key        INT REFERENCES dim_date(date_key),
        customer_key    INT REFERENCES dim_customer(customer_key),
        product_key     INT REFERENCES dim_product(product_key),
        seller_key      INT REFERENCES dim_seller(seller_key),
        order_id        VARCHAR(64),
        price           DOUBLE PRECISION,
        freight_value   DOUBLE PRECISION,
        payment_value   DOUBLE PRECISION,
        review_score    DOUBLE PRECISION
    );
    """,

    # Indexes on foreign keys for query performance
    "CREATE INDEX IF NOT EXISTS idx_fact_sales_date     ON fact_sales(date_key);",
    "CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_key);",
    "CREATE INDEX IF NOT EXISTS idx_fact_sales_product  ON fact_sales(product_key);",
    "CREATE INDEX IF NOT EXISTS idx_fact_sales_seller   ON fact_sales(seller_key);",
]


def setup_database(config: dict) -> None:
    db = config["database"]
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["name"],
        user=db["user"],
        password=db["password"],
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("Creating star schema tables...")
    for stmt in DDL_STATEMENTS:
        cur.execute(stmt)

    cur.close()
    conn.close()
    print("Database setup complete.")


if __name__ == "__main__":
    cfg = load_config()
    setup_database(cfg)
