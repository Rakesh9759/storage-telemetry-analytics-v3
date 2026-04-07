import pandas as pd
from sqlalchemy import inspect, text

from storage_telemetry.storage.db_connection import get_engine


def write_to_db(df: pd.DataFrame, table_name: str, if_exists="append"):
    engine = get_engine()
    with engine.begin() as conn:
        # Keep table objects stable so dependent SQL views are not broken.
        if if_exists == "replace":
            table_exists = inspect(conn).has_table(table_name)
            if table_exists:
                conn.execute(text(f'DELETE FROM "{table_name}"'))
                df.to_sql(table_name, conn, if_exists="append", index=False)
            else:
                df.to_sql(table_name, conn, if_exists="fail", index=False)
            return

        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
