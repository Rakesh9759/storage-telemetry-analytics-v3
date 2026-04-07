"""Anomaly detection pipeline that writes mart-compatible anomaly_events."""

import argparse
import json
import os

import pandas as pd
import yaml
from sqlalchemy import text

from storage_telemetry.detection.root_cause_rules import derive_root_cause_hint
from storage_telemetry.detection.severity import assign_severity as _assign_severity_df
from storage_telemetry.storage.db_connection import get_engine


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for anomaly stage."""
    parser = argparse.ArgumentParser(description="Detect anomalies from curated metrics")
    parser.add_argument("--ingest-run-id", default=None, help="Optional ingest run ID to process")
    return parser.parse_args()


def load_db_config(config_path: str) -> dict:
    """Load postgres config from YAML."""
    with open(config_path, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    return config.get("database", {}).get("postgres", config.get("postgres", {}))


def get_latest_curated_ingest_run_id() -> str | None:
    """Read latest ingest_run_id available in curated_device_metrics."""
    engine = get_engine()
    latest = pd.read_sql(
        """
        SELECT ingest_run_id
        FROM curated_device_metrics
        WHERE ingest_run_id IS NOT NULL
        ORDER BY ctid DESC
        LIMIT 1
        """,
        engine,
    )
    if latest.empty:
        return None
    return str(latest.iloc[0, 0])


def fetch_curated_metrics(ingest_run_id: str | None) -> pd.DataFrame:
    """Read curated metrics from Postgres, optionally scoped to one ingest run."""
    engine = get_engine()
    if not ingest_run_id:
        return pd.read_sql("SELECT * FROM curated_device_metrics", engine)

    run_sql = ingest_run_id.replace("'", "''")
    query = f"SELECT * FROM curated_device_metrics WHERE ingest_run_id = '{run_sql}'"
    return pd.read_sql(query, engine)


def _severity_for_zscore(z_score_abs: float) -> str:
    """Map z-score magnitude to severity label via the canonical severity module."""
    row = pd.DataFrame([{"detector_type": "rolling_zscore", "anomaly_score": z_score_abs}])
    return _assign_severity_df(row)["severity"].iloc[0]


def compute_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Detect anomalies per device and metric using both z-score and IQR."""
    if df.empty:
        return pd.DataFrame()

    metric_candidates = [
        "avg_latency_ms",
        "saturation_score",
        "merge_efficiency",
        "queue_efficiency",
        "iowait_pressure",
        "total_iops",
        "total_throughput_mb_s",
    ]
    metrics = [m for m in metric_candidates if m in df.columns]

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    events = []
    for device, group in df.groupby("device"):
        for metric in metrics:
            series = pd.to_numeric(group[metric], errors="coerce")
            if series.notna().sum() < 5:
                continue

            mean = float(series.mean())
            std = float(series.std(ddof=0))
            q1 = float(series.quantile(0.25))
            q3 = float(series.quantile(0.75))
            iqr = q3 - q1
            lower = q1 - (1.5 * iqr)
            upper = q3 + (1.5 * iqr)

            for _, row in group.iterrows():
                value = pd.to_numeric(row.get(metric), errors="coerce")
                if pd.isna(value):
                    continue

                z_score = 0.0 if std == 0 else float((value - mean) / std)
                z_abs = abs(z_score)
                iqr_flag = bool(value < lower or value > upper)
                z_flag = bool(z_abs >= 3.0)

                if not (z_flag or iqr_flag):
                    continue

                sev = _severity_for_zscore(z_abs)
                detector_type = "zscore+iqr" if (z_flag and iqr_flag) else ("zscore" if z_flag else "iqr")

                details = {
                    "mean": mean,
                    "std": std,
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr,
                    "lower_bound": lower,
                    "upper_bound": upper,
                    "z_score": z_score,
                }

                events.append(
                    {
                        "device": device,
                        "timestamp": row.get("timestamp"),
                        "metric_name": metric,
                        "metric_value": float(value),
                        "detector_type": detector_type,
                        "anomaly_score": float(z_abs),
                        "severity": sev,
                        "is_anomaly": 1,
                        "details": json.dumps(details),
                        "source_file": row.get("source_file"),
                        "ingest_run_id": row.get("ingest_run_id"),
                        "workload_pattern": row.get("workload_pattern", "balanced"),
                        "root_cause_hint": derive_root_cause_hint(row.to_frame().T.assign(metric_name=metric).iloc[0]),
                        "util_pct": float(row.get("util_pct", 0) or 0),
                        "aqu_sz": float(row.get("aqu_sz", 0) or 0),
                        "avg_latency_ms": float(row.get("avg_latency_ms", 0) or 0),
                        "read_ratio": float(row.get("read_ratio", 0.5) or 0.5),
                        "write_ratio": float(row.get("write_ratio", 0.5) or 0.5),
                        "avg_request_size_kb": float(row.get("avg_request_size_kb", 0) or 0),
                        "total_iops": float(row.get("total_iops", 0) or 0),
                        "total_throughput_mb_s": float(row.get("total_throughput_mb_s", 0) or 0),
                        "saturation_score": float(row.get("saturation_score", 0) or 0),
                        "latency_pressure": float(row.get("latency_pressure", 0) or 0),
                    }
                )

    return pd.DataFrame(events)


def write_anomalies_to_db(anomalies: pd.DataFrame, ingest_run_id: str | None) -> None:
    """Persist anomalies to Postgres anomaly_events table in append mode."""
    if anomalies.empty:
        return

    engine = get_engine()
    with engine.begin() as conn:
        if ingest_run_id:
            conn.execute(
                text("DELETE FROM anomaly_events WHERE ingest_run_id = :run_id"),
                {"run_id": ingest_run_id},
            )
        anomalies.to_sql("anomaly_events", conn, if_exists="append", index=False)


def main() -> None:
    """Run anomaly detection pipeline from curated metrics to anomaly events."""
    _ = load_db_config(os.path.join("configs", "database.yaml"))
    args = parse_args()
    ingest_run_id = args.ingest_run_id or get_latest_curated_ingest_run_id()

    if ingest_run_id:
        print(f"Anomaly scope ingest_run_id={ingest_run_id}")
    else:
        print("Anomaly scope ingest_run_id=<none>; using full curated_device_metrics")

    curated = fetch_curated_metrics(ingest_run_id)
    anomalies = compute_anomalies(curated)
    if anomalies.empty:
        print("No anomalies detected.")
        return

    write_anomalies_to_db(anomalies, ingest_run_id)
    print(f"Wrote {len(anomalies)} anomaly events.")


if __name__ == "__main__":
    main()