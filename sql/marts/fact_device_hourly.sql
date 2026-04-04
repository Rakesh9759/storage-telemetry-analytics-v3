DROP TABLE IF EXISTS fact_device_hourly;

CREATE TABLE fact_device_hourly AS
SELECT
    c.device,
    DATE_TRUNC('hour', c.timestamp) AS hour,
    AVG(c.avg_latency_ms) AS latency_mean,
    MAX(c.avg_latency_ms) AS latency_max,
    AVG(c.total_iops) AS iops_mean,
    AVG(c.util_pct) AS util_pct_mean,
    AVG(c.aqu_sz) AS aqu_sz_mean
FROM curated_device_metrics c
GROUP BY c.device, DATE_TRUNC('hour', c.timestamp)
ORDER BY c.device, hour;
