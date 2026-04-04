import pandas as pd
from pathlib import Path


def write_to_parquet(df: pd.DataFrame, output_path: str):
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(path, index=False)
