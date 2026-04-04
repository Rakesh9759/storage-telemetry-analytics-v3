import pandas as pd
from storage_telemetry.storage.db_connection import get_engine


def read_table(table_name: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql_table(table_name, engine)
