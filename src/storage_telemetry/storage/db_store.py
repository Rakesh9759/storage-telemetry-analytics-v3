import pandas as pd
from sqlalchemy import inspect, text

from storage_telemetry.storage.db_connection import get_engine


def _sql_type_for_series(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series.dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series.dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_bool_dtype(series.dtype):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        return "TIMESTAMPTZ"
    return "TEXT"


def _ensure_table_has_columns(conn, table_name: str, df: pd.DataFrame) -> None:
    existing_cols = {col["name"] for col in inspect(conn).get_columns(table_name)}
    for column_name in df.columns:
        if column_name in existing_cols:
            continue
        sql_type = _sql_type_for_series(df[column_name])
        conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {sql_type}'))


def write_to_db(df: pd.DataFrame, table_name: str, if_exists="append"):
    engine = get_engine()
    with engine.begin() as conn:
        # Keep table objects stable so dependent SQL views are not broken.
        if if_exists == "replace":
            table_exists = inspect(conn).has_table(table_name)
            if table_exists:
                _ensure_table_has_columns(conn, table_name, df)
                conn.execute(text(f'DELETE FROM "{table_name}"'))
                df.to_sql(table_name, conn, if_exists="append", index=False)
            else:
                df.to_sql(table_name, conn, if_exists="fail", index=False)
            return

        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
