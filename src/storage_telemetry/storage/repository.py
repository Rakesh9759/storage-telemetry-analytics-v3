import pandas as pd
from sqlalchemy import text
from storage_telemetry.storage.db_connection import get_engine


def read_table(table_name: str) -> pd.DataFrame:
    engine = get_engine()
    query = text(f'SELECT * FROM "{table_name}"')
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)
