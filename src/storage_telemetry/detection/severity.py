import pandas as pd


def assign_severity(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    def classify(row):
        score = abs(row["anomaly_score"]) if pd.notnull(row["anomaly_score"]) else 0

        if row["detector_type"] == "rolling_zscore":
            if score >= 5:
                return "critical"
            if score >= 4:
                return "high"
            return "medium"

        if row["detector_type"] == "iqr":
            if score >= 5:
                return "critical"
            if score >= 3:
                return "high"
            if score >= 1.5:
                return "medium"
            return "low"

        if row["detector_type"] == "isolation_forest":
            if score >= 0.3:
                return "critical"
            if score >= 0.15:
                return "high"
            if score >= 0.05:
                return "medium"
            return "low"

        return "low"

    out["severity"] = out.apply(classify, axis=1)
    return out
