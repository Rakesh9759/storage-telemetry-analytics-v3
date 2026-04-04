from sqlalchemy import create_engine
from urllib.parse import quote_plus
from storage_telemetry.core.config import load_config


def get_engine():
    db_config = load_config("database.yaml")["database"]

    if db_config["type"] == "sqlite":
        return create_engine(f"sqlite:///{db_config['sqlite_path']}")

    if db_config["type"] == "postgres":
        pg = db_config["postgres"]
        return create_engine(
            f"postgresql://{quote_plus(pg['user'])}:{quote_plus(pg['password'])}@{pg['host']}:{pg['port']}/{pg['db']}"
        )

    raise ValueError("Unsupported database type")
