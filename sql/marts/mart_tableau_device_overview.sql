DROP TABLE IF EXISTS mart_tableau_device_overview;

CREATE TABLE mart_tableau_device_overview AS
WITH workload_counts AS (
    SELECT
        device,
        workload_pattern,
        COUNT(*) AS pattern_count,
        ROW_NUMBER() OVER (
            PARTITION BY device
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM curated_device_metrics
    GROUP BY device, workload_pattern
),
dominant_workload AS (
    SELECT
        device,
        workload_pattern AS dominant_workload_pattern
    FROM workload_counts
    WHERE rn = 1
),
anomaly_counts AS (
    SELECT
        device,
        COUNT(*) AS anomaly_count,
        SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_anomaly_count,
        SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) AS high_anomaly_count
    FROM anomaly_events
    GROUP BY device
)
SELECT
    c.device,
    COUNT(*) AS sample_count,
    AVG(c.total_iops) AS avg_total_iops,
    AVG(c.total_throughput_mb_s) AS avg_throughput_mb_s,
    AVG(c.avg_latency_ms) AS avg_latency_ms,
    AVG(c.util_pct) AS avg_util_pct,
    AVG(c.aqu_sz) AS avg_queue_depth,
    MAX(dw.dominant_workload_pattern) AS dominant_workload_pattern,
    COALESCE(MAX(a.anomaly_count), 0) AS anomaly_count,
    COALESCE(MAX(a.critical_anomaly_count), 0) AS critical_anomaly_count,
    COALESCE(MAX(a.high_anomaly_count), 0) AS high_anomaly_count
FROM curated_device_metrics c
LEFT JOIN dominant_workload dw
    ON c.device = dw.device
LEFT JOIN anomaly_counts a
    ON c.device = a.device
GROUP BY c.device;
