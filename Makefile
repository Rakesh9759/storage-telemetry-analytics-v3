PYTHON = .venv/Scripts/python.exe
PIP = .venv/Scripts/pip.exe

install:
	$(PIP) install -r requirements.txt

test:
	$(PYTHON) -m pytest
