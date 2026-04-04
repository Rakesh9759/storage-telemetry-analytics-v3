# Architecture

## Overview

Storage Telemetry Analytics is a batch-first pipeline that transforms raw Linux `iostat` device metrics into anomaly-enriched analytical datasets, dashboard-ready marts, and investigation reports.

## Pipeline Stages

```
Raw iostat CSV
    │
    ▼
┌──────────────┐
│  Ingestion   │  iostat_parser → schema_validator → batch_ingest
└──────┬───────┘
       │  raw_device_metrics (12 columns)
       ▼
┌──────────────┐
│  Curation    │  derive_features → build_curated_metrics → quality_checks
└──────┬───────┘
       │  curated_device_metrics (23 columns)
       ▼
┌──────────────┐
│  Detection   │  rolling_zscore / iqr / isolation_forest → severity → root_cause_rules → enrich
└──────┬───────┘
       │  anomaly_events
       ▼
┌──────────────────┐
│  Exports + Marts │  dashboard_views → tableau_extracts
└──────┬───────────┘
       │  mart_tableau_device_overview
       │  mart_tableau_anomaly_timeline
       │  mart_tableau_root_cause_summary
       │  v_grafana_device_health
       ▼
┌──────────────┐
│  Timeseries  │  rolling stats, hourly aggregates, percentiles
└──────┬───────┘
       ▼
┌──────────────┐
│  Reporting   │  summary_builder → recommendations → markdown + HTML
└──────────────┘
       │
       ▼
  assets/sample_reports/
```

## Module Layout

```
src/storage_telemetry/
├── core/              # Config loader, logging, constants, exceptions
├── storage/           # DB connection factory, SQLite/Parquet stores, repository, DDL
├── ingestion/         # iostat parsing, schema validation, batch ingestion
├── analytics/         # Workload pattern classifier (read-heavy, write-heavy, balanced, etc.)
├── transforms/        # Feature derivation, curated metrics builder, timeseries transforms
├── detection/         # Anomaly detectors (z-score, IQR, Isolation Forest), severity, root cause
├── exports/           # Dashboard view builders, Tableau CSV extracts
├── reporting/         # Summary aggregation, recommendations, MD/HTML rendering
└── cli.py             # CLI entry point with --mode dispatch
```

## Storage Layer

- **SQLite** (default): local warehouse at `data/warehouse/sqlite.db`
- **PostgreSQL**: optional, configured via `configs/database.yaml`
- **Parquet**: used for staging intermediate datasets under `data/staging/`
- **SQLAlchemy**: engine factory supports both backends via `get_engine()`

## Configuration

All pipeline behavior is controlled by YAML files under `configs/`:

| File | Purpose |
|------|---------|
| `app.yaml` | App name, version, environment |
| `database.yaml` | DB type, connection parameters |
| `logging.yaml` | Log level, format, output file |
| `paths.yaml` | Data directory paths |
| `anomaly.yaml` | Detector thresholds (z-score, IQR, Isolation Forest) |
| `features.yaml` | Feature derivation parameters |

## Data Flow

1. **Ingest**: Parse CSV → validate schema → write to `raw_device_metrics`
2. **Curate**: Derive features (IOPS, throughput, ratios, saturation, pressure) → write to `curated_device_metrics`
3. **Classify**: Assign workload pattern per row (read-heavy, write-heavy, balanced, saturated, latency-sensitive, idle)
4. **Detect**: Run 3 detectors per metric → assign severity → apply root-cause rules → write to `anomaly_events`
5. **Export**: Build 4 dashboard mart tables from curated + anomaly data
6. **Timeseries**: Compute rolling means/stds, hourly aggregates, percentiles
7. **Report**: Aggregate mart data → generate recommendations → render MD + HTML

## Orchestration

- **CLI** (`cli.py`): `--mode` flag dispatches to individual pipeline stages
- **Makefile**: `make pipeline` runs all stages in sequence
- **Batch script** (`scripts/run_batch_pipeline.py`): programmatic full pipeline execution
