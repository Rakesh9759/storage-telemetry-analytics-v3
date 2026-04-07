from pathlib import Path
import pandas as pd

from storage_telemetry.storage.repository import read_table
from storage_telemetry.storage.db_store import write_to_db
from storage_telemetry.storage.parquet_store import write_to_parquet
from storage_telemetry.exports.dashboard_views import (
    build_device_overview_mart,
    build_anomaly_timeline_mart,
    build_root_cause_summary_mart,
    build_grafana_device_health_view,
)
from storage_telemetry.exports.quality_checks import (
    validate_device_overview_mart,
    validate_anomaly_timeline_mart,
    validate_root_cause_summary_mart,
    validate_grafana_health_view,
)


def export_dashboard_datasets():
    curated_df = read_table("curated_device_metrics")
    anomaly_df = read_table("anomaly_events")

    curated_df["timestamp"] = pd.to_datetime(curated_df["timestamp"])
    anomaly_df["timestamp"] = pd.to_datetime(anomaly_df["timestamp"])

    device_overview = build_device_overview_mart(curated_df, anomaly_df)
    anomaly_timeline = build_anomaly_timeline_mart(anomaly_df)
    root_cause_summary = build_root_cause_summary_mart(anomaly_df)
    grafana_health = build_grafana_device_health_view(curated_df, anomaly_df)

    validate_device_overview_mart(device_overview)
    validate_anomaly_timeline_mart(anomaly_timeline)
    validate_root_cause_summary_mart(root_cause_summary)
    validate_grafana_health_view(grafana_health)

    write_to_db(device_overview, "mart_tableau_device_overview", if_exists="replace")
    write_to_db(anomaly_timeline, "mart_tableau_anomaly_timeline", if_exists="replace")
    write_to_db(root_cause_summary, "mart_tableau_root_cause_summary", if_exists="replace")

    base_dir = Path("data/curated/dashboard_exports")
    base_dir.mkdir(parents=True, exist_ok=True)

    write_to_parquet(device_overview, str(base_dir / "mart_tableau_device_overview.parquet"))
    write_to_parquet(anomaly_timeline, str(base_dir / "mart_tableau_anomaly_timeline.parquet"))
    write_to_parquet(root_cause_summary, str(base_dir / "mart_tableau_root_cause_summary.parquet"))
    write_to_parquet(grafana_health, str(base_dir / "v_grafana_device_health.parquet"))

    device_overview.to_csv(base_dir / "mart_tableau_device_overview.csv", index=False)
    anomaly_timeline.to_csv(base_dir / "mart_tableau_anomaly_timeline.csv", index=False)
    root_cause_summary.to_csv(base_dir / "mart_tableau_root_cause_summary.csv", index=False)
    grafana_health.to_csv(base_dir / "v_grafana_device_health.csv", index=False)

    print("Dashboard datasets exported successfully.")
