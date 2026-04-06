"""Validate that dashboard mart tables are populated in Postgres."""

import os

import psycopg2
import yaml


def load_db_config(config_path: str) -> dict:
    """Load postgres config from YAML file."""
    with open(config_path, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    return config.get("database", {}).get("postgres", config.get("postgres", {}))


def validate_dashboard_datasets() -> None:
    """Raise an error if any required mart/view has zero rows."""
    db = load_db_config(os.path.join("configs", "database.yaml"))
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["db"],
        user=db["user"],
        password=db["password"],
    )
    cur = conn.cursor()
    tables = [
        "mart_tableau_anomaly_timeline",
        "mart_tableau_device_overview",
        "mart_tableau_root_cause_summary",
        "v_grafana_device_health",
    ]
    for table_name in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        if count <= 0:
            raise ValueError(f"Table or view {table_name} is empty")

    cur.close()
    conn.close()


def main() -> None:
    """Entry point for CLI execution."""
    validate_dashboard_datasets()


if __name__ == "__main__":
    main()
