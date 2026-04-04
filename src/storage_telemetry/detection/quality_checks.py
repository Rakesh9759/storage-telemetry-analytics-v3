import pandas as pd


def validate_anomaly_events(df: pd.DataFrame):
    required_columns = [
        "device",
        "timestamp",
        "metric_name",
        "detector_type",
        "anomaly_score",
        "severity",
        "is_anomaly",
        "workload_pattern",
        "root_cause_hint",
    ]

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing anomaly columns: {missing}")

    if df["root_cause_hint"].isnull().any():
        raise ValueError("Null root_cause_hint values found")

    return True
