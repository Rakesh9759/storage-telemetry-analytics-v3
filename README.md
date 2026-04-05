# Storage Telemetry Analytics Platform

Batch-first analytics system for disk I/O telemetry, anomaly detection, workload classification, and performance investigation.

## Problem

Storage systems generate high-volume telemetry (IOPS, latency, queue depth, utilization), but raw metrics alone do not explain:

- when performance degrades
- which devices are affected
- why anomalies occur

This project converts raw telemetry into **interpretable analytics outputs**.

---

## What this project does

- Parses disk telemetry (`iostat -xmt`)
- Builds curated analytical datasets
- Performs statistical + ML anomaly detection
- Classifies workload patterns
- Generates root-cause hints
- Produces Tableau-ready datasets
- Generates Markdown + HTML investigation reports

---

## Architecture

Pipeline:

Synthetic Raw CSV → Load to Postgres (`raw_device_metrics`) → Spark Curation
→ Anomaly Detection (`anomaly_events`) → Dashboard Marts
→ Validation → Reporting

---

## Outputs

### Tableau
- Device overview
- Anomaly timeline
- Root-cause summary

### Grafana
- Device health monitoring
- Anomaly flags

### Reports
- Executive summary
- Affected devices
- Root-cause insights
- Recommendations

---

## Setup

Requirements:

- Python 3.9+
- PostgreSQL (required for DB-backed pipeline)
- Java runtime (required by PySpark)

```bash
python -m venv .venv
.venv\Scripts\activate    # Windows
pip install -r requirements.txt
pip install -e .
```

Initialize database tables:

```bash
python -m storage_telemetry.cli --mode init-db
```

## Run

```bash
# Run full local pipeline (generate -> load raw -> spark -> anomaly -> marts -> report)
make pipeline

# Other commands
make install    # Install dependencies
make run        # Generate report only
make test       # Run tests
make notebook   # Launch Jupyter
```

Airflow orchestration (daily schedule) is available in `dags/storage_telemetry_dag.py`.

## Project Structure

```
configs/           # YAML configuration files
data/              # Data lake (raw → staging → curated → warehouse)
src/               # Source code (ingestion, transforms, detection, exports, reporting)
sql/               # SQL views and mart definitions
scripts/           # Pipeline runner, data generator, DB inspector
notebooks/         # Jupyter analysis notebooks
dashboards/        # Grafana and Tableau wireframes
docs/              # Architecture and design documentation
assets/            # Sample generated reports
tests/             # Test suite
```
