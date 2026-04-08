CREATE OR REPLACE VIEW mart_device_run_summary AS
WITH perf AS (
    SELECT
        c.device,
        c.ingest_run_id,
        MIN(c.timestamp) AS run_time,
        ROUND(AVG(c.avg_latency_ms)::numeric, 2) AS avg_latency_ms,
        ROUND(MAX(c.avg_latency_ms)::numeric, 2) AS max_latency_ms,
        ROUND(AVG(c.util_pct)::numeric, 2) AS avg_util_pct,
        ROUND(MAX(c.util_pct)::numeric, 2) AS max_util_pct,
        ROUND(AVG(c.total_iops)::numeric, 0) AS avg_iops,
        ROUND(AVG(c.total_throughput_mb_s)::numeric, 2) AS avg_throughput_mb_s,
        ROUND(AVG(c.saturation_score)::numeric, 3) AS avg_saturation_score,
        ROUND(AVG(c.aqu_sz)::numeric, 3) AS avg_aqu_sz,
        MODE() WITHIN GROUP (ORDER BY c.workload_pattern) AS dominant_workload_pattern
    FROM curated_device_metrics c
    GROUP BY c.device, c.ingest_run_id
),
anomaly_rollup AS (
    SELECT
        a.device,
        a.ingest_run_id,
        COUNT(*) AS total_anomalies,
        SUM(CASE WHEN a.severity = 'critical' THEN 1 ELSE 0 END) AS critical_count,
        SUM(CASE WHEN a.severity = 'high' THEN 1 ELSE 0 END) AS high_count
    FROM anomaly_events a
    GROUP BY a.device, a.ingest_run_id
),
root_ranked AS (
    SELECT
        a.device,
        a.ingest_run_id,
        a.root_cause_hint,
        COUNT(*) AS root_cause_events,
        ROW_NUMBER() OVER (
            PARTITION BY a.device, a.ingest_run_id
            ORDER BY COUNT(*) DESC, a.root_cause_hint
        ) AS rn
    FROM anomaly_events a
    GROUP BY a.device, a.ingest_run_id, a.root_cause_hint
),
top_root AS (
    SELECT
        device,
        ingest_run_id,
        root_cause_hint AS top_root_cause
    FROM root_ranked
    WHERE rn = 1
)
SELECT
    p.device,
    p.ingest_run_id,
    p.run_time,
    p.avg_latency_ms,
    p.max_latency_ms,
    p.avg_util_pct,
    p.max_util_pct,
    p.avg_iops,
    p.avg_throughput_mb_s,
    p.avg_saturation_score,
    p.avg_aqu_sz,
    p.dominant_workload_pattern,
    COALESCE(ar.total_anomalies, 0) AS total_anomalies,
    COALESCE(ar.critical_count, 0) AS critical_count,
    COALESCE(ar.high_count, 0) AS high_count,
    COALESCE(tr.top_root_cause, 'No anomalies detected') AS top_root_cause
FROM perf p
LEFT JOIN anomaly_rollup ar
    ON p.device = ar.device AND p.ingest_run_id = ar.ingest_run_id
LEFT JOIN top_root tr
    ON p.device = tr.device AND p.ingest_run_id = tr.ingest_run_id;