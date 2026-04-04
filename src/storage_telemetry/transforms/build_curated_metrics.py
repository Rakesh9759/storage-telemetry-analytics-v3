from storage_telemetry.core.config import load_config
from storage_telemetry.storage.repository import read_table
from storage_telemetry.storage.db_store import write_to_db
from storage_telemetry.storage.parquet_store import write_to_parquet
from storage_telemetry.transforms.derive_features import derive_features
from storage_telemetry.transforms.quality_checks import validate_curated_metrics
from storage_telemetry.analytics.workload_classifier import add_workload_patterns


def build_curated_metrics():
    raw_df = read_table("raw_device_metrics")
    curated_df = derive_features(raw_df)
    curated_df = add_workload_patterns(curated_df)
    validate_curated_metrics(curated_df)

    write_to_db(curated_df, "curated_device_metrics", if_exists="replace")
    write_to_parquet(
        curated_df,
        "data/curated/device_metrics/curated_device_metrics.parquet"
    )

    print(f"Curated metrics built: {len(curated_df)} rows")
