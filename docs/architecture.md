# Architecture

## Overview

Storage Telemetry Analytics is a batch-first pipeline that transforms raw Linux `iostat` device metrics into anomaly-enriched analytical datasets, dashboard-ready marts, and investigation reports.

## Pipeline Stages

```
Raw iostat CSV
    │
    ▼
┌──────────────┐
│  Ingestion   │  scripts/generate_sample_data.py → raw_device_metrics
└──────┬───────┘
       │  raw_device_metrics (base + extended telemetry fields)
       ▼
┌──────────────┐
│  Curation    │  pipelines/spark_transform.py (PySpark feature engineering)
└──────┬───────┘
       │  curated_device_metrics (legacy + extended derived features)
       ▼
┌──────────────┐
│  Detection   │  pipelines/anomaly_detection.py (Z-score + IQR + root-cause hints)
└──────┬───────┘
       │  anomaly_events
       ▼
┌──────────────────┐
│  Exports + Marts │  pipelines/build_marts.py → tableau_extracts
└──────┬───────────┘
       │  mart_tableau_device_overview
       │  mart_tableau_anomaly_timeline
       │  mart_tableau_root_cause_summary
       │  v_grafana_device_health
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
├── ingestion/         # iostat parsing and schema validation helpers
├── analytics/         # Workload pattern classifier (read-heavy, write-heavy, balanced, etc.)
├── transforms/        # Reusable feature derivation and quality checks
├── detection/         # Anomaly detectors (z-score, IQR, Isolation Forest), severity, root cause
├── exports/           # Dashboard view builders, Tableau CSV extracts
├── reporting/         # Summary aggregation, recommendations, MD/HTML rendering
└── cli.py             # CLI entry point (report, init-db)
```

## Storage Layer

- **PostgreSQL**: configured via `configs/database.yaml`
- **Parquet**: used for staging intermediate datasets under `data/staging/`
- **SQLAlchemy**: engine factory for PostgreSQL via `get_engine()`

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

1. **Generate + Ingest**: Generate raw telemetry CSV and load to `raw_device_metrics`
2. **Curate (Spark)**: Apply feature engineering in PySpark → write `curated_device_metrics`
3. **Detect**: Score anomalies (Z-score + IQR), assign severity/root-cause hints → write `anomaly_events`
4. **Export**: Build dashboard marts from curated + anomaly data
5. **Validate**: Assert mart/view tables are non-empty
6. **Report**: Aggregate mart data → generate recommendations → render MD + HTML

## Orchestration

- **Airflow DAG** (`dags/storage_telemetry_dag.py`): daily orchestration and retries
- **Makefile**: `make pipeline` runs the Spark/Postgres path locally
- **Pipeline scripts** (`pipelines/*.py`): transform, anomaly detection, marts, and validation stages
