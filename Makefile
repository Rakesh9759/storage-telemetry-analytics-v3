PYTHON = .venv/Scripts/python.exe
PIP = .venv/Scripts/pip.exe

install:
	$(PIP) install -r requirements.txt

test:
	$(PYTHON) -m pytest

run:
	$(PYTHON) -m storage_telemetry.cli --mode report

pipeline:
	$(PYTHON) scripts/generate_sample_data.py
	$(PYTHON) pipelines/load_raw_to_postgres.py
	spark-submit pipelines/spark_transform.py
	$(PYTHON) pipelines/anomaly_detection.py
	$(PYTHON) pipelines/build_marts.py
	$(PYTHON) -m storage_telemetry.cli --mode report

notebook:
	$(PYTHON) -m jupyter notebook
