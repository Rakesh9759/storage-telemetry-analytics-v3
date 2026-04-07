from sqlalchemy import text
from storage_telemetry.storage.db_connection import get_engine


def run_sql_file(path):
    engine = get_engine()
    with open(path, "r") as f:
        sql = f.read()

    with engine.begin() as conn:
        conn.execute(text(sql))


def reset_relation_for_view(view_name: str) -> None:
    """Drop same-named table/view so CREATE OR REPLACE VIEW can succeed."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {view_name} CASCADE"))
        conn.execute(text(f"DROP VIEW IF EXISTS {view_name} CASCADE"))


def init_db():
    prefix = "src/storage_telemetry/storage/ddl/postgres"
    run_sql_file(f"{prefix}_schema.sql")
    run_sql_file(f"{prefix}_curated_schema.sql")
    run_sql_file(f"{prefix}_anomaly_schema.sql")
    run_sql_file(f"{prefix}_marts_schema.sql")

    # Ensure these are true SQL views, not legacy tables from earlier runs.
    reset_relation_for_view("v_curated_device_metrics")
    reset_relation_for_view("v_grafana_device_health")
    run_sql_file("sql/views/v_curated_device_metrics.sql")
    run_sql_file("sql/views/v_grafana_device_health.sql")

    print("Database initialized.")
