from sqlalchemy import create_engine
from urllib.parse import quote_plus
from storage_telemetry.core.config import load_config


def get_engine():
    db_config = load_config("database.yaml")["database"]
    pg = db_config["postgres"]
    return create_engine(
        f"postgresql+psycopg2://{quote_plus(pg['user'])}:{quote_plus(pg['password'])}@{pg['host']}:{pg['port']}/{pg['db']}"
    )
