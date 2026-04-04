DROP TABLE IF EXISTS fact_device_percentiles;

CREATE TABLE fact_device_percentiles AS
SELECT
    c.device,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY c.avg_latency_ms) AS p50,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY c.avg_latency_ms) AS p95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY c.avg_latency_ms) AS p99
FROM curated_device_metrics c
GROUP BY c.device
ORDER BY c.device;
