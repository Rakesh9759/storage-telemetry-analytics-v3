"""Load generated raw telemetry CSV into Postgres raw_device_metrics."""

import argparse
import uuid

import pandas as pd

from storage_telemetry.storage.db_store import write_to_db


RAW_CSV_PATH = "data/raw/generated_iostat.csv"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for raw load pipeline."""
    parser = argparse.ArgumentParser(description="Load raw telemetry into Postgres")
    parser.add_argument("--ingest-run-id", default=None, help="Optional ingest run ID")
    return parser.parse_args()


def main() -> None:
    """Read raw CSV, attach ingest metadata, and write to raw_device_metrics."""
    args = parse_args()
    ingest_run_id = args.ingest_run_id or str(uuid.uuid4())

    df = pd.read_csv(RAW_CSV_PATH)
    df["source_file"] = RAW_CSV_PATH
    df["ingest_run_id"] = ingest_run_id
    write_to_db(df, "raw_device_metrics", if_exists="append")
    print(f"Appended {len(df)} rows into raw_device_metrics (ingest_run_id={ingest_run_id})")


if __name__ == "__main__":
    main()
