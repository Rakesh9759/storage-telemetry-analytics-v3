from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from storage_telemetry.core.config import load_config


def create_database():
    db_config = load_config("database.yaml")["database"]

    if db_config["type"] != "postgres":
        print("SQLite does not require explicit database creation.")
        return

    pg = db_config["postgres"]
    engine = create_engine(
        f"postgresql://{quote_plus(pg['user'])}:{quote_plus(pg['password'])}@{pg['host']}:{pg['port']}/postgres",
        isolation_level="AUTOCOMMIT",
    )

    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE {pg['db']}"))

    print(f"Database '{pg['db']}' created successfully.")


if __name__ == "__main__":
    create_database()
