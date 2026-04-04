import uuid
from pathlib import Path

from storage_telemetry.ingestion.iostat_parser import parse_iostat_file
from storage_telemetry.ingestion.schema_validator import validate_schema
from storage_telemetry.storage.db_store import write_to_db
from storage_telemetry.storage.parquet_store import write_to_parquet
from storage_telemetry.core.config import load_config


def run_batch_ingestion(file_path: str):
    ingest_id = str(uuid.uuid4())

    df = parse_iostat_file(file_path)

    validate_schema(df)

    df["source_file"] = file_path
    df["ingest_run_id"] = ingest_id

    write_to_db(df, "raw_device_metrics", if_exists="replace")

    parquet_path = f"data/staging/parsed_metrics/{ingest_id}.parquet"
    write_to_parquet(df, parquet_path)

    print(f"Ingestion complete: {len(df)} rows")
