PYTHON = .venv/Scripts/python.exe
PIP = .venv/Scripts/pip.exe

install:
	$(PIP) install -r requirements.txt

test:
	$(PYTHON) -m pytest

run:
	$(PYTHON) -m storage_telemetry.cli --mode report

pipeline:
	$(PYTHON) -m storage_telemetry.cli --mode ingest --file data/raw/generated_iostat.csv
	$(PYTHON) -m storage_telemetry.cli --mode curate
	$(PYTHON) -m storage_telemetry.cli --mode detect
	$(PYTHON) -m storage_telemetry.cli --mode export
	$(PYTHON) -m storage_telemetry.cli --mode timeseries
	$(PYTHON) -m storage_telemetry.cli --mode report

notebook:
	$(PYTHON) -m jupyter notebook
