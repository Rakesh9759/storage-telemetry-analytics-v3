# Storage Telemetry Analytics

A batch-first storage telemetry analytics platform that ingests Linux `iostat` device metrics, detects anomalies, classifies workloads, and generates investigation reports.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate    # Windows
pip install -r requirements.txt
pip install -e .
```

## Usage

```bash
make test       # Run tests
```

## Project Structure

```
configs/           # YAML configuration files
data/              # Data lake (raw → staging → curated → warehouse)
src/               # Source code
tests/             # Test suite
```
