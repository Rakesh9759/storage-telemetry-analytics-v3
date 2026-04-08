CREATE OR REPLACE VIEW mart_tableau_root_cause_summary AS
SELECT
	ingest_run_id,
	root_cause_hint,
	workload_pattern,
	COUNT(*) AS anomaly_count,
	SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_count,
	SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) AS high_count,
	COUNT(DISTINCT device) AS affected_devices,
	ROUND(AVG(anomaly_score)::numeric, 3) AS avg_anomaly_score
FROM anomaly_events
GROUP BY ingest_run_id, root_cause_hint, workload_pattern
ORDER BY ingest_run_id, critical_count DESC, anomaly_count DESC;
