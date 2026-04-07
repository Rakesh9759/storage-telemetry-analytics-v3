CREATE OR REPLACE VIEW v_grafana_device_health AS
WITH flags AS (
    SELECT
        device,
        timestamp,
        MAX(is_anomaly) AS anomaly_flag,
        MAX(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_flag,
        MAX(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) AS high_flag
    FROM anomaly_events
    GROUP BY device, timestamp
)
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
    COALESCE(f.anomaly_flag, 0) AS anomaly_flag,
    COALESCE(f.critical_flag, 0) AS critical_flag,
    COALESCE(f.high_flag, 0) AS high_flag
FROM curated_device_metrics c
LEFT JOIN flags f
    ON c.device = f.device
    AND c.timestamp = f.timestamp;
