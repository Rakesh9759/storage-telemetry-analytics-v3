"""Shared helper functions for project notebooks."""

from pathlib import Path
from urllib.parse import quote_plus

import yaml
from sqlalchemy import create_engine


def load_db_config(config_path: str = "../configs/database.yaml") -> dict:
	"""Load database configuration YAML and return the postgres section."""
	cfg_path = Path(config_path)
	with cfg_path.open("r", encoding="utf-8") as f:
		config = yaml.safe_load(f)
	return config["database"]["postgres"]


def build_postgres_conn_str(pg_conf: dict) -> str:
	"""Build a SQLAlchemy connection string for PostgreSQL."""
	pg_user = quote_plus(pg_conf["user"])
	pg_password = quote_plus(pg_conf["password"])
	pg_host = pg_conf["host"]
	pg_port = pg_conf["port"]
	pg_db = pg_conf["db"]
	return f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"


def get_postgres_engine(config_path: str = "../configs/database.yaml"):
	"""Create and return (engine, postgres_config) for notebooks."""
	pg_conf = load_db_config(config_path=config_path)
	conn_str = build_postgres_conn_str(pg_conf)
	engine = create_engine(conn_str, pool_pre_ping=True)
	return engine, pg_conf
