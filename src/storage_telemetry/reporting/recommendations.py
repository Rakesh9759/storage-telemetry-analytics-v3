def generate_recommendations(summary: dict) -> list[str]:
    recommendations = []

    if summary["critical_anomalies"] > 0:
        recommendations.append(
            "Review critical anomaly windows first, prioritizing devices with repeated latency and saturation signals."
        )

    if summary["avg_util_pct"] >= 80:
        recommendations.append(
            "Investigate sustained high utilization periods to determine whether capacity pressure or burst traffic is driving stress."
        )

    if any("saturation" in rc["root_cause_hint"].lower() for rc in summary["top_root_causes"]):
        recommendations.append(
            "Correlate high-utilization and queue-depth periods with latency spikes to validate saturation-driven degradation."
        )

    if any("write-heavy" in rc["root_cause_hint"].lower() for rc in summary["top_root_causes"]):
        recommendations.append(
            "Inspect write-heavy windows for flush pressure, burst ingestion patterns, or checkpoint-like activity."
        )

    if not recommendations:
        recommendations.append(
            "Review anomaly distribution by device and time to confirm whether flagged behavior reflects isolated spikes or recurring degradation."
        )

    return recommendations
