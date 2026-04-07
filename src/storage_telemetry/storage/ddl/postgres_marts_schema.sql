CREATE TABLE IF NOT EXISTS mart_run_summary (
    id SERIAL PRIMARY KEY,
    ingest_run_id TEXT NOT NULL,
    run_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_rows INTEGER NOT NULL,
    curated_rows INTEGER NOT NULL,
    anomaly_rows INTEGER NOT NULL,
    affected_devices INTEGER NOT NULL,
    critical_anomalies INTEGER NOT NULL,
    high_anomalies INTEGER NOT NULL,
    max_anomaly_score REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mart_run_summary_ingest_run_id
ON mart_run_summary (ingest_run_id);