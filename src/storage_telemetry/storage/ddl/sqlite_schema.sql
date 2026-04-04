CREATE TABLE IF NOT EXISTS raw_device_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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

    source_file TEXT,
    ingest_run_id TEXT
);
