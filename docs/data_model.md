# Data Model

## Tables

### raw_device_metrics

Ingested directly from parsed iostat CSV. One row per device per timestamp.

| Column | Type | Description |
|--------|------|-------------|
| device | text | Device name (e.g., sda, nvme0n1) |
| timestamp | datetime | Sample timestamp |
| r_s | float | Reads per second |
| w_s | float | Writes per second |
| rmb_s | float | Read throughput (MB/s) |
| wmb_s | float | Write throughput (MB/s) |
| r_await | float | Read latency (ms) |
| w_await | float | Write latency (ms) |
| aqu_sz | float | Average queue size |
| rrqm_s | float | Read merges per second |
| wrqm_s | float | Write merges per second |
| rareq_sz | float | Average read request size (KB) |
| wareq_sz | float | Average write request size (KB) |
| svctm | float | Service time (ms) |
| iowait_pct | float | CPU iowait percentage |
| util_pct | float | Device utilization (%) |

### curated_device_metrics

Enriched with derived features and workload classification.

| Column | Type | Description |
|--------|------|-------------|
| *(all raw columns)* | | Inherited from raw_device_metrics |
| total_iops | float | r_s + w_s |
| total_throughput_mb_s | float | rmb_s + wmb_s |
| avg_latency_ms | float | Weighted average of r_await and w_await |
| read_ratio | float | r_s / total_iops |
| write_ratio | float | w_s / total_iops |
| avg_request_size_kb | float | Weighted average request size |
| saturation_score | float | Composite metric: util_pct + aqu_sz × latency |
| io_intensity | float | total_iops × total_throughput_mb_s |
| latency_pressure | float | avg_latency_ms × (1 + aqu_sz) |
| merge_rate_total | float | rrqm_s + wrqm_s |
| merge_efficiency | float | merge_rate_total / (total_iops + merge_rate_total) |
| await_ratio | float | w_await / r_await |
| svctm_await_ratio | float | svctm / avg_latency_ms |
| queue_efficiency | float | total_iops / aqu_sz |
| write_amplification | float | w_s / r_s |
| iowait_pressure | float | iowait_pct × util_pct / 100 |
| iops_total | float | Alias of total_iops |
| throughput_mb_s | float | Alias of total_throughput_mb_s |
| weighted_avg_latency | float | Alias of avg_latency_ms |
| hour_of_day | int | Extracted from timestamp |
| day_of_week | int | Extracted from timestamp |
| workload_pattern | text | Classified: read-heavy, write-heavy, balanced, saturated, latency-sensitive, idle |

### anomaly_events

One row per detected anomaly per device per metric per timestamp.

| Column | Type | Description |
|--------|------|-------------|
| device | text | Device name |
| timestamp | datetime | When the anomaly occurred |
| metric_name | text | Which metric triggered (avg_latency_ms, util_pct, etc.) |
| metric_value | float | Observed value at detection time |
| detector_type | text | Which detector flagged it (zscore, iqr, or zscore+iqr) |
| anomaly_score | float | Detector-specific score |
| severity | text | critical, high, low |
| is_anomaly | bool | True if flagged |
| workload_pattern | text | Workload at time of anomaly |
| root_cause_hint | text | Human-readable root-cause explanation |
| details | text | Additional detector context |
| source_file | text | Original ingested file |
| ingest_run_id | text | Batch run identifier |

---

## Mart Tables

### mart_tableau_device_overview

One row per device. Aggregated metrics with anomaly counts and dominant workload.

| Column | Type | Description |
|--------|------|-------------|
| device | text | Device name |
| sample_count | int | Number of curated rows |
| avg_total_iops | float | Mean IOPS |
| avg_throughput_mb_s | float | Mean throughput |
| avg_latency_ms | float | Mean latency |
| avg_util_pct | float | Mean utilization |
| avg_queue_depth | float | Mean queue size |
| dominant_workload_pattern | text | Most frequent workload |
| anomaly_count | int | Total anomalies |
| critical_anomaly_count | int | Critical severity count |
| high_anomaly_count | int | High severity count |

### mart_tableau_anomaly_timeline

One row per anomaly event with enriched context for drilldown.

| Column | Type | Description |
|--------|------|-------------|
| device | text | Device name |
| timestamp | datetime | Event time |
| metric_name | text | Triggering metric |
| detector_type | text | Detection method |
| severity | text | Severity level |
| anomaly_score | float | Score |
| workload_pattern | text | Workload at time |
| root_cause_hint | text | Root-cause signal |
| util_pct | float | Device utilization |
| aqu_sz | float | Queue depth |
| avg_latency_ms | float | Latency |
| total_iops | float | IOPS |
| saturation_score | float | Saturation metric |

### mart_tableau_root_cause_summary

Aggregated root-cause signals by workload pattern and ingest run.

| Column | Type | Description |
|--------|------|-------------|
| ingest_run_id | text | Source ingest run for this aggregate |
| root_cause_hint | text | Root-cause description |
| workload_pattern | text | Associated workload |
| anomaly_count | int | Total anomalies with this cause |
| critical_count | int | Critical anomalies with this cause |
| high_count | int | High anomalies with this cause |
| affected_devices | int | Distinct devices affected by this cause |
| avg_anomaly_score | float | Average anomaly score for this cause/workload/run |

### v_grafana_device_health

Real-time device health view with anomaly flags for Grafana panels.

| Column | Type | Description |
|--------|------|-------------|
| device | text | Device name |
| timestamp | datetime | Sample time |
| total_iops | float | IOPS |
| total_throughput_mb_s | float | Throughput |
| avg_latency_ms | float | Latency |
| util_pct | float | Utilization |
| aqu_sz | float | Queue depth |
| saturation_score | float | Saturation metric |
| latency_pressure | float | Latency pressure metric |
| anomaly_flag | bool | Any anomaly detected |
| critical_flag | bool | Critical anomaly detected |
| high_flag | bool | High severity anomaly detected |

---

## Data Lake Structure

```
data/
├── raw/                  # Original ingested files (CSV)
├── staging/              # Reserved for intermediate files
├── curated/              # Exported analytical datasets
│   ├── dashboard_exports/
│   └── generated_iostat_curated.csv
└── warehouse/
```
