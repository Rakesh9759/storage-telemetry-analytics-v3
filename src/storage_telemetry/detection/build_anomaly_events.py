import pandas as pd

from storage_telemetry.core.config import load_config
from storage_telemetry.storage.repository import read_table
from storage_telemetry.storage.db_store import write_to_db
from storage_telemetry.storage.parquet_store import write_to_parquet
from storage_telemetry.detection.rolling_baselines import add_rolling_baselines
from storage_telemetry.detection.rolling_zscore import detect_rolling_zscore
from storage_telemetry.detection.iqr_detector import detect_iqr
from storage_telemetry.detection.isolation_forest_detector import detect_isolation_forest
from storage_telemetry.detection.severity import assign_severity
from storage_telemetry.detection.quality_checks import validate_anomaly_events
from storage_telemetry.detection.enrich_anomaly_events import enrich_anomaly_events


DETECTION_METRICS = [
    "avg_latency_ms",
    "total_iops",
    "util_pct",
    "aqu_sz",
    "saturation_score",
    "latency_pressure",
]


def build_anomaly_events():
    db_config = load_config("database.yaml")
    anomaly_config = load_config("anomaly.yaml")

    curated_df = read_table("curated_device_metrics")

    curated_df["timestamp"] = pd.to_datetime(curated_df["timestamp"])

    baseline_df = add_rolling_baselines(
        curated_df,
        metric_cols=DETECTION_METRICS,
        window=anomaly_config.get("rolling_window", 5)
    )

    zscore_df = detect_rolling_zscore(
        baseline_df,
        metric_cols=DETECTION_METRICS,
        threshold=anomaly_config["zscore"]["threshold"]
    )

    iqr_df = detect_iqr(
        curated_df,
        metric_cols=DETECTION_METRICS,
        multiplier=anomaly_config["iqr"]["multiplier"]
    )

    iforest_df = detect_isolation_forest(
        curated_df,
        metric_cols=DETECTION_METRICS,
        contamination=anomaly_config.get("isolation_forest", {}).get("contamination", 0.05)
    )

    anomaly_df = pd.concat([zscore_df, iqr_df, iforest_df], ignore_index=True)

    if anomaly_df.empty:
        print("No anomaly events detected.")
        return

    anomaly_df = assign_severity(anomaly_df)
    anomaly_df = enrich_anomaly_events(anomaly_df, curated_df)

    validate_anomaly_events(anomaly_df)

    write_to_db(anomaly_df, "anomaly_events", if_exists="replace")

    write_to_parquet(
        anomaly_df,
        "data/curated/anomaly_events/anomaly_events.parquet"
    )

    print(f"Anomaly events built: {len(anomaly_df)} rows")
