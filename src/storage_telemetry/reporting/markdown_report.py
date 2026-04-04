from datetime import datetime


def render_markdown_report(summary: dict, recommendations: list[str]) -> str:
    lines = []

    lines.append("# Storage Telemetry Investigation Summary")
    lines.append("")
    lines.append(f"Generated at: {datetime.utcnow().isoformat()} UTC")
    lines.append("")

    lines.append("## 1. Run Overview")
    lines.append("")
    lines.append(f"- Total devices analyzed: **{summary['total_devices']}**")
    lines.append(f"- Total anomalies detected: **{summary['total_anomalies']}**")
    lines.append(f"- Critical anomalies: **{summary['critical_anomalies']}**")
    lines.append(f"- High severity anomalies: **{summary['high_anomalies']}**")
    lines.append(f"- Average device latency: **{summary['avg_latency_ms']} ms**")
    lines.append(f"- Average device utilization: **{summary['avg_util_pct']}%**")
    lines.append("")

    lines.append("## 2. Executive Summary")
    lines.append("")
    lines.append(
        "This report summarizes storage telemetry behavior across analyzed devices, "
        "with emphasis on anomaly frequency, severity distribution, workload patterns, "
        "and likely root-cause signals."
    )
    lines.append("")

    lines.append("## 3. Dominant Workload Patterns")
    lines.append("")
    if summary["dominant_workloads"]:
        for workload, count in summary["dominant_workloads"].items():
            lines.append(f"- **{workload}**: {count} device(s)")
    else:
        lines.append("- No workload pattern summary available.")
    lines.append("")

    lines.append("## 4. Most Affected Devices")
    lines.append("")
    if summary["most_affected_devices"]:
        for item in summary["most_affected_devices"]:
            lines.append(
                f"- **{item['device']}** — anomalies: {item['anomaly_count']}, "
                f"critical: {item['critical_anomaly_count']}, "
                f"dominant workload: {item['dominant_workload_pattern']}, "
                f"avg latency: {round(item['avg_latency_ms'], 2)} ms"
            )
    else:
        lines.append("- No affected device summary available.")
    lines.append("")

    lines.append("## 5. Top Root-Cause Signals")
    lines.append("")
    if summary["top_root_causes"]:
        for item in summary["top_root_causes"]:
            lines.append(
                f"- **{item['root_cause_hint']}** "
                f"(workload: {item['workload_pattern']}, "
                f"anomalies: {item['anomaly_count']}, "
                f"critical: {item['critical_count']})"
            )
    else:
        lines.append("- No root-cause summary available.")
    lines.append("")

    lines.append("## 6. Recommended Next Checks")
    lines.append("")
    for rec in recommendations:
        lines.append(f"- {rec}")
    lines.append("")

    return "\n".join(lines)
