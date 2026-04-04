import pandas as pd


def detect_iqr(
    df: pd.DataFrame,
    metric_cols: list[str],
    multiplier: float = 1.5
) -> pd.DataFrame:
    events = []

    for device, device_df in df.groupby("device"):
        for metric in metric_cols:
            q1 = device_df[metric].quantile(0.25)
            q3 = device_df[metric].quantile(0.75)
            iqr = q3 - q1

            lower = q1 - multiplier * iqr
            upper = q3 + multiplier * iqr

            flagged = device_df[
                (device_df[metric] < lower) | (device_df[metric] > upper)
            ]

            for _, row in flagged.iterrows():
                score = 0.0
                if row[metric] > upper and iqr != 0:
                    score = (row[metric] - upper) / iqr
                elif row[metric] < lower and iqr != 0:
                    score = (lower - row[metric]) / iqr

                events.append({
                    "device": row["device"],
                    "timestamp": row["timestamp"],
                    "metric_name": metric,
                    "metric_value": row[metric],
                    "detector_type": "iqr",
                    "anomaly_score": float(score),
                    "severity": None,
                    "is_anomaly": 1,
                    "details": f"lower={lower:.2f}, upper={upper:.2f}",
                    "source_file": row.get("source_file"),
                    "ingest_run_id": row.get("ingest_run_id"),
                })

    return pd.DataFrame(events)
