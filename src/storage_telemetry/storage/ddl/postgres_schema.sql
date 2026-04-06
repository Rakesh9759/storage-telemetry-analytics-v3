CREATE TABLE IF NOT EXISTS raw_device_metrics (
    id SERIAL PRIMARY KEY,
    device TEXT NOT NULL,
    timestamp TEXT NOT NULL,

    r_s REAL,
    w_s REAL,
    rmb_s REAL,
    wmb_s REAL,
    r_await REAL,
    w_await REAL,
    aqu_sz REAL,
    util_pct REAL,
    rrqm_s REAL,
    wrqm_s REAL,
    rareq_sz REAL,
    wareq_sz REAL,
    svctm REAL,
    iowait_pct REAL,

    source_file TEXT,
    ingest_run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_device_metrics_device
ON raw_device_metrics (device);

CREATE INDEX IF NOT EXISTS idx_raw_device_metrics_timestamp
ON raw_device_metrics (timestamp);

CREATE INDEX IF NOT EXISTS idx_raw_device_metrics_device_timestamp
ON raw_device_metrics (device, timestamp);

CREATE INDEX IF NOT EXISTS idx_raw_device_metrics_ingest_run_id
ON raw_device_metrics (ingest_run_id);
