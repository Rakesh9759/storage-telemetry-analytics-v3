SELECT
    device,
    timestamp,
    metric_name,
    detector_type,
    severity,
    anomaly_score,
    workload_pattern,
    root_cause_hint,
    util_pct,
    aqu_sz,
    avg_latency_ms,
    total_iops,
    saturation_score
FROM mart_tableau_anomaly_timeline;
