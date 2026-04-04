import pandas as pd


def build_report_summary(
    device_overview: pd.DataFrame,
    anomaly_timeline: pd.DataFrame,
    root_cause_summary: pd.DataFrame
) -> dict:
    total_devices = int(device_overview["device"].nunique()) if not device_overview.empty else 0
    total_anomalies = int(len(anomaly_timeline))
    critical_anomalies = int((anomaly_timeline["severity"] == "critical").sum()) if "severity" in anomaly_timeline.columns else 0
    high_anomalies = int((anomaly_timeline["severity"] == "high").sum()) if "severity" in anomaly_timeline.columns else 0

    avg_latency = float(device_overview["avg_latency_ms"].mean()) if not device_overview.empty else 0.0
    avg_util = float(device_overview["avg_util_pct"].mean()) if not device_overview.empty else 0.0

    most_affected_devices = []
    if not device_overview.empty and "anomaly_count" in device_overview.columns:
        most_affected_devices = (
            device_overview.sort_values(["critical_anomaly_count", "anomaly_count"], ascending=False)
            [["device", "anomaly_count", "critical_anomaly_count", "dominant_workload_pattern", "avg_latency_ms"]]
            .head(5)
            .to_dict(orient="records")
        )

    top_root_causes = []
    if not root_cause_summary.empty:
        top_root_causes = (
            root_cause_summary.sort_values(["critical_count", "anomaly_count"], ascending=False)
            [["root_cause_hint", "workload_pattern", "anomaly_count", "critical_count"]]
            .head(5)
            .to_dict(orient="records")
        )

    dominant_workloads = []
    if not device_overview.empty and "dominant_workload_pattern" in device_overview.columns:
        dominant_workloads = (
            device_overview["dominant_workload_pattern"]
            .value_counts()
            .head(5)
            .to_dict()
        )

    return {
        "total_devices": total_devices,
        "total_anomalies": total_anomalies,
        "critical_anomalies": critical_anomalies,
        "high_anomalies": high_anomalies,
        "avg_latency_ms": round(avg_latency, 2),
        "avg_util_pct": round(avg_util, 2),
        "most_affected_devices": most_affected_devices,
        "top_root_causes": top_root_causes,
        "dominant_workloads": dominant_workloads,
    }
