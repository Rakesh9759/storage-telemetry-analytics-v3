import yaml
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]

def load_config(file_name: str):
    path = BASE_DIR / "configs" / file_name
    with open(path, "r") as f:
        return yaml.safe_load(f)
