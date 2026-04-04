import pandas as pd


def validate_curated_metrics(df: pd.DataFrame):
    required_columns = [
        "device",
        "timestamp",
        "total_iops",
        "total_throughput_mb_s",
        "avg_latency_ms",
        "saturation_score",
        "workload_pattern",
    ]

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing curated columns: {missing}")

    if df["total_iops"].isnull().any():
        raise ValueError("Null values found in total_iops")

    if (df["util_pct"] < 0).any() or (df["util_pct"] > 100).any():
        raise ValueError("util_pct out of range")

    allowed_patterns = {
        "read_heavy",
        "write_heavy",
        "balanced",
        "high_throughput",
        "small_io_pressure",
        "saturated",
        "latency_sensitive",
        "burst_io",
    }

    invalid = set(df["workload_pattern"].dropna().unique()) - allowed_patterns
    if invalid:
        raise ValueError(f"Invalid workload patterns found: {invalid}")

    return True
