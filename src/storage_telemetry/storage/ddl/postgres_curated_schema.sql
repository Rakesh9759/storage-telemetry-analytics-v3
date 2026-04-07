CREATE TABLE IF NOT EXISTS curated_device_metrics (
    id SERIAL PRIMARY KEY,
    device TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,

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
    iops_total REAL,
    total_throughput_mb_s REAL,
    throughput_mb_s REAL,
    avg_latency_ms REAL,
    weighted_avg_latency REAL,
    read_ratio REAL,
    write_ratio REAL,
    avg_request_size_kb REAL,
    saturation_score REAL,
    io_intensity REAL,
    latency_pressure REAL,
    merge_rate_total REAL,
    merge_efficiency REAL,
    await_ratio REAL,
    svctm_await_ratio REAL,
    queue_efficiency REAL,
    write_amplification REAL,
    iowait_pressure REAL,
    workload_pattern TEXT,
    rrqm_s REAL,
    wrqm_s REAL,
    rareq_sz REAL,
    wareq_sz REAL,
    svctm REAL,
    iowait_pct REAL,
    hour_of_day INTEGER,
    day_of_week INTEGER
);

CREATE INDEX IF NOT EXISTS idx_curated_device_metrics_device
ON curated_device_metrics (device);

CREATE INDEX IF NOT EXISTS idx_curated_device_metrics_timestamp
ON curated_device_metrics (timestamp);

CREATE INDEX IF NOT EXISTS idx_curated_device_metrics_device_timestamp
ON curated_device_metrics (device, timestamp);

CREATE INDEX IF NOT EXISTS idx_curated_device_metrics_workload_pattern
ON curated_device_metrics (workload_pattern);

CREATE INDEX IF NOT EXISTS idx_curated_device_metrics_ingest_run_id
ON curated_device_metrics (ingest_run_id);
