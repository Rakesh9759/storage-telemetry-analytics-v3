import pandas as pd


def validate_device_overview_mart(df: pd.DataFrame):
    required = [
        "device",
        "sample_count",
        "avg_total_iops",
        "avg_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "dominant_workload_pattern",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing device overview columns: {missing}")
    return True


def validate_anomaly_timeline_mart(df: pd.DataFrame):
    required = [
        "device",
        "timestamp",
        "metric_name",
        "severity",
        "root_cause_hint",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing anomaly timeline columns: {missing}")
    return True


def validate_root_cause_summary_mart(df: pd.DataFrame):
    required = [
        "ingest_run_id",
        "root_cause_hint",
        "workload_pattern",
        "anomaly_count",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing root cause summary columns: {missing}")
    return True


def validate_device_run_summary_mart(df: pd.DataFrame):
    required = [
        "device",
        "ingest_run_id",
        "run_time",
        "avg_latency_ms",
        "avg_util_pct",
        "avg_iops",
        "avg_throughput_mb_s",
        "avg_saturation_score",
        "dominant_workload_pattern",
        "total_anomalies",
        "critical_count",
        "high_count",
        "top_root_cause",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing device run summary columns: {missing}")
    return True


def validate_grafana_health_view(df: pd.DataFrame):
    required = [
        "device",
        "timestamp",
        "avg_latency_ms",
        "util_pct",
        "aqu_sz",
        "anomaly_flag",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing Grafana health columns: {missing}")
    return True
