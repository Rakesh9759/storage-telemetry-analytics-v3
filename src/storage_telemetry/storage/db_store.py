import pandas as pd
from storage_telemetry.storage.db_connection import get_engine


def write_to_db(df: pd.DataFrame, table_name: str, if_exists="append"):
    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
