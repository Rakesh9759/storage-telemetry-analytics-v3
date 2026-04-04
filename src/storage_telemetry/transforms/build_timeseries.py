import pandas as pd
from storage_telemetry.storage.db_store import write_to_db


def build_timeseries(curated_df):
    cols = [
        "device", "timestamp",
        "total_iops",
        "total_throughput_mb_s",
        "avg_latency_ms",
        "util_pct",
        "aqu_sz",
        "saturation_score",
        "latency_pressure",
        "read_ratio",
        "write_ratio"
    ]

    df = curated_df[cols].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Step 2 — Rolling features
    df["latency_rolling_mean"] = df.groupby("device")["avg_latency_ms"].transform(lambda s: s.rolling(10).mean())
    df["latency_rolling_std"] = df.groupby("device")["avg_latency_ms"].transform(lambda s: s.rolling(10).std())

    # Step 3 — Hourly aggregates
    df["hour"] = df["timestamp"].dt.floor("h")

    hourly = df.groupby(["device", "hour"]).agg({
        "avg_latency_ms": ["mean", "max"],
        "total_iops": "mean",
        "util_pct": "mean",
        "aqu_sz": "mean"
    })
    hourly.columns = ["latency_mean", "latency_max", "iops_mean", "util_pct_mean", "aqu_sz_mean"]
    hourly = hourly.reset_index()

    # Step 4 — Percentiles
    percentiles = df.groupby("device")["avg_latency_ms"].quantile([0.5, 0.95, 0.99]).unstack()
    percentiles.columns = ["p50", "p95", "p99"]
    percentiles = percentiles.reset_index()

    # Step 5 — Store in DB
    write_to_db(df.drop(columns=["hour"]), "fact_device_timeseries", if_exists="replace")
    write_to_db(hourly, "fact_device_hourly", if_exists="replace")
    write_to_db(percentiles, "fact_device_percentiles", if_exists="replace")

    print(f"Timeseries: {len(df)} rows, Hourly: {len(hourly)} rows, Percentiles: {len(percentiles)} rows")

    return df
