import pandas as pd


def build_device_overview_mart(
    curated_df: pd.DataFrame,
    anomaly_df: pd.DataFrame
) -> pd.DataFrame:
    workload_mode = (
        curated_df.groupby("device")["workload_pattern"]
        .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else "unknown")
        .reset_index()
        .rename(columns={"workload_pattern": "dominant_workload_pattern"})
    )

    summary = (
        curated_df.groupby("device")
        .agg(
            sample_count=("device", "size"),
            avg_total_iops=("total_iops", "mean"),
            avg_throughput_mb_s=("total_throughput_mb_s", "mean"),
            avg_latency_ms=("avg_latency_ms", "mean"),
            p95_latency_ms=("avg_latency_ms", lambda s: s.quantile(0.95)),
            p99_latency_ms=("avg_latency_ms", lambda s: s.quantile(0.99)),
            avg_util_pct=("util_pct", "mean"),
            avg_queue_depth=("aqu_sz", "mean"),
        )
        .reset_index()
    )

    anomaly_summary = (
        anomaly_df.groupby("device")
        .agg(
            anomaly_count=("device", "size"),
            critical_anomaly_count=("severity", lambda s: (s == "critical").sum()),
            high_anomaly_count=("severity", lambda s: (s == "high").sum()),
        )
        .reset_index()
    )

    out = (
        summary.merge(workload_mode, on="device", how="left")
        .merge(anomaly_summary, on="device", how="left")
    )

    for col in ["anomaly_count", "critical_anomaly_count", "high_anomaly_count"]:
        out[col] = out[col].fillna(0).astype(int)

    return out


def build_anomaly_timeline_mart(anomaly_df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "device",
        "timestamp",
        "metric_name",
        "detector_type",
        "severity",
        "anomaly_score",
        "workload_pattern",
        "root_cause_hint",
        "util_pct",
        "aqu_sz",
        "avg_latency_ms",
        "total_iops",
        "saturation_score",
    ]
    existing = [c for c in cols if c in anomaly_df.columns]
    return anomaly_df[existing].copy()


def build_root_cause_summary_mart(anomaly_df: pd.DataFrame) -> pd.DataFrame:
    return (
        anomaly_df.groupby(["ingest_run_id", "root_cause_hint", "workload_pattern"])
        .agg(
            anomaly_count=("device", "size"),
            critical_count=("severity", lambda s: (s == "critical").sum()),
            high_count=("severity", lambda s: (s == "high").sum()),
            affected_devices=("device", "nunique"),
            avg_anomaly_score=("anomaly_score", "mean"),
        )
        .reset_index()
        .sort_values(["critical_count", "anomaly_count"], ascending=False)
    )


def build_device_run_summary_mart(
    curated_df: pd.DataFrame,
    anomaly_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build one-row-per-device-per-run summary for fast ad-hoc analytics."""
    perf = (
        curated_df.groupby(["device", "ingest_run_id"])
        .agg(
            run_time=("timestamp", "min"),
            avg_latency_ms=("avg_latency_ms", "mean"),
            max_latency_ms=("avg_latency_ms", "max"),
            avg_util_pct=("util_pct", "mean"),
            max_util_pct=("util_pct", "max"),
            avg_iops=("total_iops", "mean"),
            avg_throughput_mb_s=("total_throughput_mb_s", "mean"),
            avg_saturation_score=("saturation_score", "mean"),
            avg_aqu_sz=("aqu_sz", "mean"),
        )
        .reset_index()
    )

    workload = (
        curated_df.groupby(["device", "ingest_run_id"])["workload_pattern"]
        .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else "unknown")
        .reset_index()
        .rename(columns={"workload_pattern": "dominant_workload_pattern"})
    )

    anomaly_rollup = (
        anomaly_df.groupby(["device", "ingest_run_id"])
        .agg(
            total_anomalies=("device", "size"),
            critical_count=("severity", lambda s: int((s == "critical").sum())),
            high_count=("severity", lambda s: int((s == "high").sum())),
        )
        .reset_index()
    )

    top_root = (
        anomaly_df.groupby(["device", "ingest_run_id", "root_cause_hint"])
        .size()
        .reset_index(name="root_cause_events")
        .sort_values(["device", "ingest_run_id", "root_cause_events"], ascending=[True, True, False])
        .drop_duplicates(subset=["device", "ingest_run_id"], keep="first")
        .rename(columns={"root_cause_hint": "top_root_cause"})
        [["device", "ingest_run_id", "top_root_cause"]]
    )

    out = (
        perf.merge(workload, on=["device", "ingest_run_id"], how="left")
        .merge(anomaly_rollup, on=["device", "ingest_run_id"], how="left")
        .merge(top_root, on=["device", "ingest_run_id"], how="left")
    )

    for col in ["total_anomalies", "critical_count", "high_count"]:
        out[col] = out[col].fillna(0).astype(int)

    out["top_root_cause"] = out["top_root_cause"].fillna("No anomalies detected")
    return out


def build_grafana_device_health_view(
    curated_df: pd.DataFrame,
    anomaly_df: pd.DataFrame
) -> pd.DataFrame:
    base = curated_df[
        [
            "device",
            "timestamp",
            "total_iops",
            "total_throughput_mb_s",
            "avg_latency_ms",
            "util_pct",
            "aqu_sz",
            "saturation_score",
            "latency_pressure",
        ]
    ].copy()

    flags = (
        anomaly_df.groupby(["device", "timestamp"])
        .agg(
            anomaly_flag=("is_anomaly", "max"),
            critical_flag=("severity", lambda s: int((s == "critical").any())),
            high_flag=("severity", lambda s: int((s == "high").any())),
        )
        .reset_index()
    )

    out = base.merge(flags, on=["device", "timestamp"], how="left")

    for col in ["anomaly_flag", "critical_flag", "high_flag"]:
        out[col] = out[col].fillna(0).astype(int)

    return out
