CREATE OR REPLACE VIEW v_latest_run AS
SELECT
    (
        SELECT ingest_run_id
        FROM anomaly_events
        WHERE ingest_run_id IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 1
    ) AS anomaly_run_id,
    (
        SELECT ingest_run_id
        FROM curated_device_metrics
        WHERE ingest_run_id IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 1
    ) AS curated_run_id;