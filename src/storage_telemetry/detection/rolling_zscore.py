import numpy as np
import pandas as pd


def detect_rolling_zscore(
    df: pd.DataFrame,
    metric_cols: list[str],
    threshold: float = 3.0
) -> pd.DataFrame:
    events = []

    for _, row in df.iterrows():
        for metric in metric_cols:
            mean_col = f"{metric}_rolling_mean"
            std_col = f"{metric}_rolling_std"

            mean_val = row.get(mean_col)
            std_val = row.get(std_col)
            metric_val = row.get(metric)

            if pd.isna(mean_val) or pd.isna(std_val) or std_val == 0:
                continue

            zscore = (metric_val - mean_val) / std_val
            is_anomaly = abs(zscore) >= threshold

            if is_anomaly:
                events.append({
                    "device": row["device"],
                    "timestamp": row["timestamp"],
                    "metric_name": metric,
                    "metric_value": metric_val,
                    "detector_type": "rolling_zscore",
                    "anomaly_score": float(zscore),
                    "severity": None,
                    "is_anomaly": 1,
                    "details": f"zscore={zscore:.2f}",
                    "source_file": row.get("source_file"),
                    "ingest_run_id": row.get("ingest_run_id"),
                })

    return pd.DataFrame(events)
