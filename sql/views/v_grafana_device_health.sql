SELECT
    device,
    timestamp,
    total_iops,
    total_throughput_mb_s,
    avg_latency_ms,
    util_pct,
    aqu_sz,
    saturation_score,
    latency_pressure,
    anomaly_flag,
    critical_flag,
    high_flag
FROM v_grafana_device_health;
