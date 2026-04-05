"""Anomaly detection pipeline that writes mart-compatible anomaly_events."""

import json
import os

import numpy as np
import pandas as pd
import yaml

from storage_telemetry.storage.db_connection import get_engine


def load_db_config(config_path: str) -> dict:
    """Load postgres config from YAML."""
    with open(config_path, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    return config["postgres"]


def fetch_curated_metrics() -> pd.DataFrame:
    """Read curated metrics from Postgres."""
    engine = get_engine()
    return pd.read_sql("SELECT * FROM curated_device_metrics", engine)


def assign_severity(z_score_abs: float) -> str:
    """Map z-score magnitude to severity."""
    if z_score_abs >= 6:
        return "critical"
    if z_score_abs >= 4:
        return "high"
    return "low"


def root_cause_hint(metric_name: str, row: pd.Series) -> str:
    """Generate actionable root-cause hint from metric and contextual signals."""
    util = float(row.get("util_pct", 0) or 0)
    queue = float(row.get("aqu_sz", 0) or 0)
    iowait_pressure = float(row.get("iowait_pressure", 0) or 0)
    avg_req_kb = float(row.get("avg_request_size_kb", 0) or 0)

    if metric_name in {"saturation_score", "avg_latency_ms"} and util > 90 and queue > 5:
        return "Device saturation with queue buildup"
    if metric_name in {"merge_efficiency", "avg_latency_ms"} and avg_req_kb < 8:
        return "Small random I/O pressure reducing merge effectiveness"
    if metric_name == "iowait_pressure" and iowait_pressure > 20:
        return "CPU I/O wait pressure indicates backend storage bottleneck"
    if metric_name == "queue_efficiency" and queue > 3:
        return "Queue depth increased while throughput efficiency degraded"
    return f"{metric_name} deviated from device baseline"


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
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

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

            for idx, row in group.iterrows():
                value = pd.to_numeric(row.get(metric), errors="coerce")
                if pd.isna(value):
                    continue

                z_score = 0.0 if std == 0 else float((value - mean) / std)
                z_abs = abs(z_score)
                iqr_flag = bool(value < lower or value > upper)
                z_flag = bool(z_abs >= 3.0)

                if not (z_flag or iqr_flag):
                    continue

                sev = assign_severity(z_abs)
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
                        "root_cause_hint": root_cause_hint(metric, row),
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


def write_anomalies_to_db(anomalies: pd.DataFrame) -> None:
    """Persist anomalies to Postgres anomaly_events table."""
    engine = get_engine()
    anomalies.to_sql("anomaly_events", engine, if_exists="replace", index=False)


def main() -> None:
    """Run anomaly detection pipeline from curated metrics to anomaly events."""
    _ = load_db_config(os.path.join("configs", "database.yaml"))
    curated = fetch_curated_metrics()
    anomalies = compute_anomalies(curated)
    if anomalies.empty:
        print("No anomalies detected.")
        return

    write_anomalies_to_db(anomalies)
    print(f"Wrote {len(anomalies)} anomaly events.")


if __name__ == "__main__":
    main()
