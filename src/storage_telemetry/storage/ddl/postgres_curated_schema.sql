CREATE TABLE IF NOT EXISTS curated_device_metrics (
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

    source_file TEXT,
    ingest_run_id TEXT,

    total_iops REAL,
    total_throughput_mb_s REAL,
    avg_latency_ms REAL,
    read_ratio REAL,
    write_ratio REAL,
    avg_request_size_kb REAL,
    saturation_score REAL,
    io_intensity REAL,
    latency_pressure REAL,
    hour_of_day INTEGER,
    day_of_week INTEGER
);
