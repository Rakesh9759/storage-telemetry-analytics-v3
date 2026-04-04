import pandas as pd


def derive_root_cause_hint(row: pd.Series) -> str:
    metric_name = row.get("metric_name")
    metric_value = row.get("metric_value", 0.0)
    workload_pattern = row.get("workload_pattern", "unknown")
    util_pct = row.get("util_pct", 0.0)
    aqu_sz = row.get("aqu_sz", 0.0)
    avg_latency_ms = row.get("avg_latency_ms", 0.0)
    read_ratio = row.get("read_ratio", 0.0)
    write_ratio = row.get("write_ratio", 0.0)
    avg_request_size_kb = row.get("avg_request_size_kb", 0.0)

    if metric_name == "avg_latency_ms":
        if aqu_sz >= 2 and util_pct >= 80:
            return "Latency spike likely driven by saturation and queue buildup"
        if write_ratio >= 0.7:
            return "Latency degradation likely under write-heavy pressure"
        if read_ratio >= 0.7:
            return "Latency spike observed during read-heavy demand"
        return "Latency anomaly detected without clear saturation signal"

    if metric_name == "util_pct":
        if util_pct >= 90:
            return "Device operating near full utilization"
        return "Utilization anomaly detected"

    if metric_name == "aqu_sz":
        if avg_latency_ms >= 10:
            return "Queue buildup likely contributing to latency pressure"
        return "Queue depth anomaly detected"

    if metric_name == "saturation_score":
        return "Composite saturation signal indicates elevated device stress"

    if metric_name == "latency_pressure":
        return "Joint increase in latency and queue depth suggests pressure buildup"

    if metric_name == "total_iops":
        if avg_request_size_kb <= 16:
            return "High IOPS with small requests suggests random IO pressure"
        return "IOPS anomaly indicates bursty load increase"

    if metric_name == "multivariate":
        if workload_pattern == "saturated":
            return "Multivariate anomaly aligned with saturation pattern"
        if workload_pattern == "write_heavy":
            return "Multivariate anomaly aligned with write-heavy stress"
        if workload_pattern == "read_heavy":
            return "Multivariate anomaly aligned with read-heavy load"
        return "Multivariate anomaly indicates unusual joint metric behavior"

    return "Anomalous behavior detected"
