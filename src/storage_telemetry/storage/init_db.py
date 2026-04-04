from sqlalchemy import text
from storage_telemetry.storage.db_connection import get_engine
from storage_telemetry.core.config import load_config


def run_sql_file(path):
    engine = get_engine()
    with open(path, "r") as f:
        sql = f.read()

    with engine.begin() as conn:
        conn.execute(text(sql))


def init_db():
    db_type = load_config("database.yaml")["database"]["type"]

    if db_type == "postgres":
        prefix = "src/storage_telemetry/storage/ddl/postgres"
    else:
        prefix = "src/storage_telemetry/storage/ddl/sqlite"

    run_sql_file(f"{prefix}_schema.sql")
    run_sql_file(f"{prefix}_curated_schema.sql")
    run_sql_file(f"{prefix}_anomaly_schema.sql")

    print("Database initialized.")
