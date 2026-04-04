import pandas as pd

def validate_report_inputs(device_overview, anomaly_timeline, root_cause_summary):
    if device_overview.empty:
        raise ValueError("Device overview mart is empty")

    required_device_cols = ["device", "avg_latency_ms", "anomaly_count"]
    required_anomaly_cols = ["device", "timestamp", "severity"]
    required_root_cols = ["root_cause_hint", "workload_pattern", "anomaly_count"]

    missing_device = [c for c in required_device_cols if c not in device_overview.columns]
    missing_anomaly = [c for c in required_anomaly_cols if c not in anomaly_timeline.columns]
    missing_root = [c for c in required_root_cols if c not in root_cause_summary.columns]

    if missing_device:
        raise ValueError(f"Missing device overview columns: {missing_device}")
    if missing_anomaly:
        raise ValueError(f"Missing anomaly timeline columns: {missing_anomaly}")
    if missing_root:
        raise ValueError(f"Missing root cause summary columns: {missing_root}")

    return True
