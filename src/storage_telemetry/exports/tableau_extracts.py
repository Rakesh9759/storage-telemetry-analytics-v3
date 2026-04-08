from pathlib import Path
import pandas as pd
from sqlalchemy import inspect, text

from storage_telemetry.storage.repository import read_table
from storage_telemetry.storage.db_store import write_to_db
from storage_telemetry.storage.db_connection import get_engine
from storage_telemetry.storage.parquet_store import write_to_parquet
from storage_telemetry.exports.dashboard_views import (
    build_device_overview_mart,
    build_anomaly_timeline_mart,
    build_root_cause_summary_mart,
    build_device_run_summary_mart,
    build_grafana_device_health_view,
)
from storage_telemetry.exports.quality_checks import (
    validate_device_overview_mart,
    validate_anomaly_timeline_mart,
    validate_root_cause_summary_mart,
    validate_device_run_summary_mart,
    validate_grafana_health_view,
)


def _write_run_summary(
    ingest_run_id: str,
    curated_df: pd.DataFrame,
    anomaly_df: pd.DataFrame,
) -> None:
    """Upsert one row of run-level aggregate stats for trend monitoring."""
    raw_rows = len(read_table("raw_device_metrics").query("ingest_run_id == @ingest_run_id"))
    curated_rows = len(curated_df)
    anomaly_rows = len(anomaly_df)
    affected_devices = anomaly_df["device"].nunique() if "device" in anomaly_df.columns else 0
    critical_anomalies = int((anomaly_df["severity"] == "critical").sum()) if "severity" in anomaly_df.columns else 0
    high_anomalies = int((anomaly_df["severity"] == "high").sum()) if "severity" in anomaly_df.columns else 0
    max_anomaly_score = float(anomaly_df["anomaly_score"].max()) if "anomaly_score" in anomaly_df.columns and not anomaly_df.empty else None

    run_row = pd.DataFrame(
        [
            {
                "ingest_run_id": ingest_run_id,
                "raw_rows": raw_rows,
                "curated_rows": curated_rows,
                "anomaly_rows": anomaly_rows,
                "affected_devices": affected_devices,
                "critical_anomalies": critical_anomalies,
                "high_anomalies": high_anomalies,
                "max_anomaly_score": max_anomaly_score,
            }
        ]
    )

    engine = get_engine()
    with engine.begin() as conn:
        if inspect(conn).has_table("mart_run_summary"):
            conn.execute(text("DELETE FROM mart_run_summary WHERE ingest_run_id = :run_id"), {"run_id": ingest_run_id})
            run_row.to_sql("mart_run_summary", conn, if_exists="append", index=False)
        else:
            run_row.to_sql("mart_run_summary", conn, if_exists="fail", index=False)


def _refresh_latest_run_view() -> None:
    """Create or replace helper view with latest curated/anomaly run IDs."""
    engine = get_engine()
    sql = text(
        """
        CREATE OR REPLACE VIEW v_latest_run AS
        SELECT
            (SELECT ingest_run_id
             FROM anomaly_events
             WHERE ingest_run_id IS NOT NULL
             ORDER BY timestamp DESC
             LIMIT 1) AS anomaly_run_id,
            (SELECT ingest_run_id
             FROM curated_device_metrics
             WHERE ingest_run_id IS NOT NULL
             ORDER BY timestamp DESC
             LIMIT 1) AS curated_run_id;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql)


def export_dashboard_datasets(ingest_run_id: str | None = None):
    curated_df = read_table("curated_device_metrics")
    anomaly_df = read_table("anomaly_events")
    all_curated_df = curated_df.copy()
    all_anomaly_df = anomaly_df.copy()

    if ingest_run_id and "ingest_run_id" in curated_df.columns:
        curated_df = curated_df[curated_df["ingest_run_id"] == ingest_run_id].copy()
    if ingest_run_id and "ingest_run_id" in anomaly_df.columns:
        anomaly_df = anomaly_df[anomaly_df["ingest_run_id"] == ingest_run_id].copy()

    if curated_df.empty or anomaly_df.empty:
        raise ValueError(f"No data found for ingest_run_id={ingest_run_id}")

    curated_df["timestamp"] = pd.to_datetime(curated_df["timestamp"])
    anomaly_df["timestamp"] = pd.to_datetime(anomaly_df["timestamp"])

    device_overview = build_device_overview_mart(curated_df, anomaly_df)
    anomaly_timeline = build_anomaly_timeline_mart(anomaly_df)
    root_cause_summary = build_root_cause_summary_mart(anomaly_df)
    device_run_summary = build_device_run_summary_mart(curated_df, anomaly_df)
    grafana_health = build_grafana_device_health_view(curated_df, anomaly_df)

    validate_device_overview_mart(device_overview)
    validate_anomaly_timeline_mart(anomaly_timeline)
    validate_root_cause_summary_mart(root_cause_summary)
    validate_device_run_summary_mart(device_run_summary)
    validate_grafana_health_view(grafana_health)

    write_to_db(device_overview, "mart_tableau_device_overview", if_exists="replace")
    write_to_db(anomaly_timeline, "mart_tableau_anomaly_timeline", if_exists="replace")
    write_to_db(root_cause_summary, "mart_tableau_root_cause_summary", if_exists="replace")
    write_to_db(device_run_summary, "mart_device_run_summary", if_exists="replace")
    _refresh_latest_run_view()

    base_dir = Path("data/curated/dashboard_exports")
    base_dir.mkdir(parents=True, exist_ok=True)

    run_suffix = ingest_run_id if ingest_run_id else "latest"

    write_to_parquet(device_overview, str(base_dir / f"mart_tableau_device_overview_{run_suffix}.parquet"))
    write_to_parquet(anomaly_timeline, str(base_dir / f"mart_tableau_anomaly_timeline_{run_suffix}.parquet"))
    write_to_parquet(root_cause_summary, str(base_dir / f"mart_tableau_root_cause_summary_{run_suffix}.parquet"))
    write_to_parquet(device_run_summary, str(base_dir / f"mart_device_run_summary_{run_suffix}.parquet"))
    write_to_parquet(grafana_health, str(base_dir / f"v_grafana_device_health_{run_suffix}.parquet"))

    # Keep legacy stable filenames for current dashboards.
    write_to_parquet(device_overview, str(base_dir / "mart_tableau_device_overview.parquet"))
    write_to_parquet(anomaly_timeline, str(base_dir / "mart_tableau_anomaly_timeline.parquet"))
    write_to_parquet(root_cause_summary, str(base_dir / "mart_tableau_root_cause_summary.parquet"))
    write_to_parquet(device_run_summary, str(base_dir / "mart_device_run_summary.parquet"))
    write_to_parquet(grafana_health, str(base_dir / "v_grafana_device_health.parquet"))

    device_overview.to_csv(base_dir / f"mart_tableau_device_overview_{run_suffix}.csv", index=False)
    anomaly_timeline.to_csv(base_dir / f"mart_tableau_anomaly_timeline_{run_suffix}.csv", index=False)
    root_cause_summary.to_csv(base_dir / f"mart_tableau_root_cause_summary_{run_suffix}.csv", index=False)
    device_run_summary.to_csv(base_dir / f"mart_device_run_summary_{run_suffix}.csv", index=False)
    grafana_health.to_csv(base_dir / f"v_grafana_device_health_{run_suffix}.csv", index=False)

    device_overview.to_csv(base_dir / "mart_tableau_device_overview.csv", index=False)
    anomaly_timeline.to_csv(base_dir / "mart_tableau_anomaly_timeline.csv", index=False)
    root_cause_summary.to_csv(base_dir / "mart_tableau_root_cause_summary.csv", index=False)
    device_run_summary.to_csv(base_dir / "mart_device_run_summary.csv", index=False)
    grafana_health.to_csv(base_dir / "v_grafana_device_health.csv", index=False)

    if ingest_run_id:
        _write_run_summary(ingest_run_id, curated_df, anomaly_df)
    elif "ingest_run_id" in all_curated_df.columns and "ingest_run_id" in all_anomaly_df.columns:
        run_ids = sorted(
            set(all_curated_df["ingest_run_id"].dropna().unique())
            & set(all_anomaly_df["ingest_run_id"].dropna().unique())
        )
        for run_id in run_ids:
            _write_run_summary(
                run_id,
                all_curated_df[all_curated_df["ingest_run_id"] == run_id].copy(),
                all_anomaly_df[all_anomaly_df["ingest_run_id"] == run_id].copy(),
            )

    print(
        "Dashboard datasets exported successfully"
        + (f" for ingest_run_id={ingest_run_id}." if ingest_run_id else ".")
    )
