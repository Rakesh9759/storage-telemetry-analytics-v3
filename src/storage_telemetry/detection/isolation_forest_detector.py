import pandas as pd
from sklearn.ensemble import IsolationForest


def detect_isolation_forest(
    df: pd.DataFrame,
    metric_cols: list[str],
    contamination: float = 0.05,
    random_state: int = 42
) -> pd.DataFrame:
    work_df = df.copy()

    feature_df = work_df[metric_cols].fillna(0)

    if len(feature_df) < 10:
        return pd.DataFrame(columns=[
            "device", "timestamp", "metric_name", "metric_value",
            "detector_type", "anomaly_score", "severity",
            "is_anomaly", "details", "source_file", "ingest_run_id"
        ])

    model = IsolationForest(
        contamination=contamination,
        random_state=random_state
    )

    preds = model.fit_predict(feature_df)
    scores = model.decision_function(feature_df)

    work_df["iforest_pred"] = preds
    work_df["iforest_score"] = scores

    flagged = work_df[work_df["iforest_pred"] == -1]

    events = []
    for _, row in flagged.iterrows():
        anomaly_score = float(-row["iforest_score"])
        events.append({
            "device": row["device"],
            "timestamp": row["timestamp"],
            "metric_name": "multivariate",
            "metric_value": anomaly_score,
            "detector_type": "isolation_forest",
            "anomaly_score": anomaly_score,
            "severity": None,
            "is_anomaly": 1,
            "details": f"metrics={','.join(metric_cols)}",
            "source_file": row.get("source_file"),
            "ingest_run_id": row.get("ingest_run_id"),
        })

    return pd.DataFrame(events)