CREATE OR REPLACE VIEW v_curated_device_metrics AS
SELECT
    device,
    timestamp,
    total_iops,
    total_throughput_mb_s,
    avg_latency_ms,
    read_ratio,
    write_ratio,
    avg_request_size_kb,
    saturation_score,
    io_intensity,
    latency_pressure,
    hour_of_day,
    day_of_week,
    util_pct,
    aqu_sz
FROM curated_device_metrics;
