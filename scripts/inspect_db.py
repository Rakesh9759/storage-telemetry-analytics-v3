"""
Inspect the database: list tables, row counts, and sample rows.
Works with both SQLite and PostgreSQL backends.
"""

import pandas as pd
from sqlalchemy import inspect as sa_inspect, text
from storage_telemetry.storage.db_connection import get_engine


def main():
    engine = get_engine()
    inspector = sa_inspect(engine)

    tables = sorted(inspector.get_table_names())
    print("=== TABLES ===")
    for t in tables:
        print(f"  {t}")

    print("\n=== ROW COUNTS ===")
    with engine.connect() as conn:
        for t in tables:
            count = conn.execute(text(f'SELECT COUNT(*) FROM "{t}"')).scalar()
            print(f"  {t}: {count} rows")

    sample_tables = [
        "raw_device_metrics",
        "curated_device_metrics",
        "anomaly_events",
        "mart_tableau_device_overview",
    ]
    with engine.connect() as conn:
        for t in sample_tables:
            if t in tables:
                print(f"\n=== {t} (first 3 rows) ===")
                df = pd.read_sql(text(f'SELECT * FROM "{t}" LIMIT 3'), conn)
                print(df.to_string(index=False))


if __name__ == "__main__":
    main()
