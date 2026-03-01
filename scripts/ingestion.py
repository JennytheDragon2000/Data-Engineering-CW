"""
ingestion.py — Reads raw CSVs and writes Parquet files to the bronze layer.
Run: python scripts/ingestion.py
"""

import sys
import os
import logging
from datetime import date

import pandas as pd
import yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def load_config() -> dict:
    config_path = os.path.join(PROJECT_ROOT, "config", "config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def ingest(config: dict) -> None:
    raw_dir = os.path.join(PROJECT_ROOT, config["paths"]["raw_data"])
    bronze_dir = os.path.join(PROJECT_ROOT, config["paths"]["bronze"])
    ingestion_date = date.today().isoformat()  # e.g. "2026-03-01"

    for filename in config["datasets"]:
        csv_path = os.path.join(raw_dir, filename)

        if not os.path.exists(csv_path):
            logger.warning("File not found, skipping: %s", csv_path)
            continue

        table_name = filename.replace(".csv", "")
        logger.info("Ingesting %s ...", filename)

        df = pd.read_csv(csv_path, low_memory=False)
        row_count = len(df)
        logger.info("  Rows read: %d", row_count)

        # Add ingestion metadata
        df["_ingestion_date"] = ingestion_date

        # Partition output directory by ingestion date for incremental loads
        output_dir = os.path.join(bronze_dir, table_name, f"ingestion_date={ingestion_date}")
        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(output_dir, f"{table_name}.parquet")
        df.to_parquet(output_path, index=False, engine="pyarrow")

        file_size_kb = os.path.getsize(output_path) / 1024
        logger.info("  Written to: %s (%.1f KB)", output_path, file_size_kb)

    logger.info("Ingestion complete.")


if __name__ == "__main__":
    cfg = load_config()
    ingest(cfg)
