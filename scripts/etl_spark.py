"""
etl_spark.py — PySpark ETL: bronze → silver (cleaning) → gold (PostgreSQL DW load).
Run: spark-submit scripts/etl_spark.py
"""

import os
import sys
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DoubleType
from pyspark.sql.window import Window

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with open(os.path.join(PROJECT_ROOT, "config", "config.yaml")) as f:
        return yaml.safe_load(f)


def get_spark(config: dict) -> SparkSession:
    spark_cfg = config["spark"]
    return (
        SparkSession.builder
        .appName(spark_cfg["app_name"])
        .master(spark_cfg["master"])
        .config("spark.driver.memory", spark_cfg["driver_memory"])
        # PostgreSQL JDBC driver must be on the classpath; users add via --jars
        .getOrCreate()
    )


def latest_bronze_path(bronze_root: str, table_name: str) -> str:
    """Return the most recent ingestion partition for a bronze table."""
    table_dir = os.path.join(bronze_root, table_name)
    if not os.path.isdir(table_dir):
        raise FileNotFoundError(f"Bronze table not found: {table_dir}")
    partitions = sorted(os.listdir(table_dir))
    return os.path.join(table_dir, partitions[-1])


def jdbc_url(db: dict) -> str:
    return f"jdbc:postgresql://{db['host']}:{db['port']}/{db['name']}"


def jdbc_props(db: dict) -> dict:
    return {"user": db["user"], "password": db["password"], "driver": "org.postgresql.Driver"}


# ---------------------------------------------------------------------------
# Bronze → Silver  (4 transformations)
# ---------------------------------------------------------------------------

def clean_customers(df: DataFrame) -> DataFrame:
    # 1. Deduplication
    df = df.dropDuplicates(["customer_id"])
    # 2. Drop rows missing critical keys
    df = df.dropna(subset=["customer_id"])
    # 4. Standardise city/state to uppercase
    df = df.withColumn("customer_city", F.upper(F.col("customer_city")))
    df = df.withColumn("customer_state", F.upper(F.col("customer_state")))
    return df


def clean_orders(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates(["order_id"])
    df = df.dropna(subset=["order_id", "customer_id"])
    # 3. Parse timestamp strings
    for col_name in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.to_timestamp(col_name).cast(TimestampType()))
    return df


def clean_order_items(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates()
    df = df.dropna(subset=["order_id", "product_id"])
    # 3. Cast numeric columns
    df = df.withColumn("price", F.col("price").cast(DoubleType()))
    df = df.withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
    if "shipping_limit_date" in df.columns:
        df = df.withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date").cast(TimestampType()))
    return df


def clean_payments(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates()
    df = df.dropna(subset=["order_id"])
    df = df.withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
    return df


def clean_reviews(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates(["review_id"])
    # 2. Fill missing review_score with median
    median_score = df.approxQuantile("review_score", [0.5], 0.01)[0]
    df = df.fillna({"review_score": median_score})
    df = df.withColumn("review_score", F.col("review_score").cast(DoubleType()))
    return df


def clean_products(df: DataFrame, translations: DataFrame) -> DataFrame:
    df = df.dropDuplicates(["product_id"])
    df = df.dropna(subset=["product_id"])
    df = df.withColumn("product_weight_g", F.col("product_weight_g").cast(DoubleType()))
    # 4. Translate category names to English
    df = df.join(
        translations.withColumnRenamed("product_category_name", "product_category_name_pt"),
        df["product_category_name"] == translations["product_category_name"],
        "left",
    ).drop("product_category_name")
    df = df.withColumnRenamed("product_category_name_english", "product_category_name")
    if "product_category_name_pt" in df.columns:
        df = df.drop("product_category_name_pt")
    return df


def clean_sellers(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates(["seller_id"])
    df = df.dropna(subset=["seller_id"])
    # 4. Standardise city/state to uppercase
    df = df.withColumn("seller_city", F.upper(F.col("seller_city")))
    df = df.withColumn("seller_state", F.upper(F.col("seller_state")))
    return df


def write_silver(df: DataFrame, silver_root: str, table_name: str) -> None:
    path = os.path.join(silver_root, table_name)
    df.write.mode("overwrite").parquet(path)
    print(f"  Silver written: {path}  ({df.count()} rows)")


# ---------------------------------------------------------------------------
# Silver → Gold  (dimension + fact loading)
# ---------------------------------------------------------------------------

def build_dim_date(orders: DataFrame) -> DataFrame:
    """Derive dim_date from all distinct purchase dates."""
    dates = (
        orders
        .select(F.to_date("order_purchase_timestamp").alias("full_date"))
        .dropna()
        .dropDuplicates()
    )
    return dates.select(
        F.col("full_date"),
        F.year("full_date").alias("year"),
        F.month("full_date").alias("month"),
        F.quarter("full_date").alias("quarter"),
        F.dayofweek("full_date").alias("day_of_week"),
    )


def upsert_dimension(df: DataFrame, table: str, url: str, props: dict) -> None:
    """Write a dimension DataFrame to PostgreSQL using INSERT … ON CONFLICT DO NOTHING."""
    # Collect to driver then bulk-insert via JDBC temp write + SQL upsert is
    # the simplest JDBC-compatible approach with PySpark
    df.write.mode("append").jdbc(url=url, table=f"{table}_staging", properties=props)


def load_dimension_jdbc(
    df: DataFrame,
    table: str,
    unique_col: str,
    url: str,
    props: dict,
) -> None:
    """
    Upsert dimension rows into PostgreSQL.
    Uses a staging table → INSERT … ON CONFLICT DO NOTHING pattern executed
    via a separate psycopg2 connection after the JDBC write.
    """
    import psycopg2

    # Parse connection details out of jdbc url / props
    # jdbc:postgresql://host:port/dbname
    rest = url.replace("jdbc:postgresql://", "")
    host_port, dbname = rest.split("/", 1)
    host, port = host_port.split(":")

    conn = psycopg2.connect(
        host=host, port=int(port), dbname=dbname,
        user=props["user"], password=props["password"]
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Create staging table matching target schema (without the serial PK)
    cur.execute(f"DROP TABLE IF EXISTS {table}_staging;")
    cur.execute(f"CREATE TABLE {table}_staging AS SELECT * FROM {table} LIMIT 0;")
    cur.execute(f"ALTER TABLE {table}_staging DROP COLUMN IF EXISTS {table.replace('dim_','')}_key;")

    # Write data into staging via JDBC
    df.write.mode("overwrite").jdbc(url=url, table=f"{table}_staging", properties=props)

    # Determine columns (all except the serial PK)
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = '{table}' AND column_name != '{table.replace("dim_","")}_key'
        ORDER BY ordinal_position;
    """)
    cols = [row[0] for row in cur.fetchall()]
    cols_str = ", ".join(cols)

    cur.execute(f"""
        INSERT INTO {table} ({cols_str})
        SELECT {cols_str} FROM {table}_staging
        ON CONFLICT ({unique_col}) DO NOTHING;
    """)

    cur.execute(f"DROP TABLE IF EXISTS {table}_staging;")
    cur.close()
    conn.close()
    print(f"  Dimension upserted: {table}")


def load_fact_sales(
    orders: DataFrame,
    items: DataFrame,
    payments: DataFrame,
    reviews: DataFrame,
    url: str,
    props: dict,
) -> None:
    """Join silver tables to build fact_sales and load to PostgreSQL."""
    import psycopg2

    # Aggregate payments per order (sum all payment installments)
    payments_agg = payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("payment_value")
    )

    # Latest review per order
    reviews_agg = (
        reviews
        .groupBy("order_id")
        .agg(F.avg("review_score").alias("review_score"))
    )

    # Join everything on order_id
    fact_raw = (
        items
        .join(orders.select("order_id", "customer_id", "order_purchase_timestamp"), "order_id")
        .join(payments_agg, "order_id", "left")
        .join(reviews_agg, "order_id", "left")
        .select(
            "order_id",
            F.to_date("order_purchase_timestamp").alias("full_date"),
            "customer_id",
            "product_id",
            "seller_id",
            F.col("price").cast(DoubleType()),
            F.col("freight_value").cast(DoubleType()),
            F.col("payment_value").cast(DoubleType()),
            F.col("review_score").cast(DoubleType()),
        )
    )

    # Resolve surrogate keys from dimension tables via JDBC lookup
    rest = url.replace("jdbc:postgresql://", "")
    host_port, dbname = rest.split("/", 1)
    host, port = host_port.split(":")

    conn = psycopg2.connect(
        host=host, port=int(port), dbname=dbname,
        user=props["user"], password=props["password"]
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Write fact_raw to staging
    cur.execute("DROP TABLE IF EXISTS fact_sales_raw_staging;")
    conn.commit()
    fact_raw.write.mode("overwrite").jdbc(
        url=url, table="fact_sales_raw_staging", properties=props
    )

    cur.execute("""
        INSERT INTO fact_sales (date_key, customer_key, product_key, seller_key,
                                order_id, price, freight_value, payment_value, review_score)
        SELECT
            d.date_key,
            c.customer_key,
            p.product_key,
            s.seller_key,
            r.order_id,
            r.price,
            r.freight_value,
            r.payment_value,
            r.review_score
        FROM fact_sales_raw_staging r
        LEFT JOIN dim_date     d ON d.full_date    = r.full_date
        LEFT JOIN dim_customer c ON c.customer_id  = r.customer_id
        LEFT JOIN dim_product  p ON p.product_id   = r.product_id
        LEFT JOIN dim_seller   s ON s.seller_id    = r.seller_id
        ON CONFLICT DO NOTHING;
    """)

    cur.execute("DROP TABLE IF EXISTS fact_sales_raw_staging;")
    cur.close()
    conn.close()
    print(f"  Fact table loaded: fact_sales")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    cfg = load_config()
    spark = get_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")

    bronze_root = os.path.join(PROJECT_ROOT, cfg["paths"]["bronze"])
    silver_root = os.path.join(PROJECT_ROOT, cfg["paths"]["silver"])
    db = cfg["database"]
    url = jdbc_url(db)
    props = jdbc_props(db)

    print("=== Reading bronze layer ===")
    customers_raw  = spark.read.parquet(latest_bronze_path(bronze_root, "olist_customers_dataset"))
    orders_raw     = spark.read.parquet(latest_bronze_path(bronze_root, "olist_orders_dataset"))
    items_raw      = spark.read.parquet(latest_bronze_path(bronze_root, "olist_order_items_dataset"))
    payments_raw   = spark.read.parquet(latest_bronze_path(bronze_root, "olist_order_payments_dataset"))
    reviews_raw    = spark.read.parquet(latest_bronze_path(bronze_root, "olist_order_reviews_dataset"))
    products_raw   = spark.read.parquet(latest_bronze_path(bronze_root, "olist_products_dataset"))
    sellers_raw    = spark.read.parquet(latest_bronze_path(bronze_root, "olist_sellers_dataset"))
    translations   = spark.read.parquet(latest_bronze_path(bronze_root, "product_category_name_translation"))

    print("\n=== Silver layer: cleaning ===")
    customers = clean_customers(customers_raw)
    orders    = clean_orders(orders_raw)
    items     = clean_order_items(items_raw)
    payments  = clean_payments(payments_raw)
    reviews   = clean_reviews(reviews_raw)
    products  = clean_products(products_raw, translations)
    sellers   = clean_sellers(sellers_raw)

    write_silver(customers, silver_root, "customers")
    write_silver(orders,    silver_root, "orders")
    write_silver(items,     silver_root, "order_items")
    write_silver(payments,  silver_root, "payments")
    write_silver(reviews,   silver_root, "reviews")
    write_silver(products,  silver_root, "products")
    write_silver(sellers,   silver_root, "sellers")

    print("\n=== Gold layer: loading dimensions ===")
    dim_date_df    = build_dim_date(orders)
    dim_customer_df = customers.select("customer_id", "customer_unique_id", "customer_city", "customer_state")
    dim_product_df  = products.select("product_id", "product_category_name", "product_weight_g")
    dim_seller_df   = sellers.select("seller_id", "seller_city", "seller_state")

    load_dimension_jdbc(dim_date_df,     "dim_date",     "full_date",    url, props)
    load_dimension_jdbc(dim_customer_df, "dim_customer", "customer_id",  url, props)
    load_dimension_jdbc(dim_product_df,  "dim_product",  "product_id",   url, props)
    load_dimension_jdbc(dim_seller_df,   "dim_seller",   "seller_id",    url, props)

    print("\n=== Gold layer: loading fact table ===")
    load_fact_sales(orders, items, payments, reviews, url, props)

    spark.stop()
    print("\nETL complete.")


if __name__ == "__main__":
    main()
