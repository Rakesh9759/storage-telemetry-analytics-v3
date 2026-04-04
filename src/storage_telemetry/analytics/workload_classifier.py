import pandas as pd


def classify_workload_pattern(row: pd.Series) -> str:
    read_ratio = row.get("read_ratio", 0.0)
    write_ratio = row.get("write_ratio", 0.0)
    avg_request_size_kb = row.get("avg_request_size_kb", 0.0)
    util_pct = row.get("util_pct", 0.0)
    aqu_sz = row.get("aqu_sz", 0.0)
    avg_latency_ms = row.get("avg_latency_ms", 0.0)
    total_iops = row.get("total_iops", 0.0)
    total_throughput_mb_s = row.get("total_throughput_mb_s", 0.0)

    if util_pct >= 85 or aqu_sz >= 3:
        return "saturated"

    if avg_latency_ms >= 10 and aqu_sz >= 1:
        return "latency_sensitive"

    if write_ratio >= 0.7:
        return "write_heavy"

    if read_ratio >= 0.7:
        return "read_heavy"

    if avg_request_size_kb <= 16 and total_iops > 0:
        return "small_io_pressure"

    if total_throughput_mb_s >= 50 and avg_latency_ms < 5:
        return "high_throughput"

    if total_iops >= 500:
        return "burst_io"

    return "balanced"


def add_workload_patterns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["workload_pattern"] = out.apply(classify_workload_pattern, axis=1)
    return out
