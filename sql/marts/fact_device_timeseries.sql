DROP TABLE IF EXISTS fact_device_timeseries;

CREATE TABLE fact_device_timeseries AS
SELECT
    c.device,
    c.timestamp,
    c.total_iops,
    c.total_throughput_mb_s,
    c.avg_latency_ms,
    c.util_pct,
    c.aqu_sz,
    c.saturation_score,
    c.latency_pressure,
    c.read_ratio,
    c.write_ratio,
    AVG(c.avg_latency_ms) OVER (
        PARTITION BY c.device
        ORDER BY c.timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS latency_rolling_mean,
    STDDEV(c.avg_latency_ms) OVER (
        PARTITION BY c.device
        ORDER BY c.timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS latency_rolling_std
FROM curated_device_metrics c
ORDER BY c.device, c.timestamp;
