import sqlite3
import pandas as pd


def write_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str, if_exists: str = "append"):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    conn.close()
