from datetime import datetime


def render_html_report(summary: dict, recommendations: list[str]) -> str:
    most_affected_html = "".join(
        [
            f"<li><strong>{item['device']}</strong> — anomalies: {item['anomaly_count']}, "
            f"critical: {item['critical_anomaly_count']}, "
            f"dominant workload: {item['dominant_workload_pattern']}, "
            f"avg latency: {round(item['avg_latency_ms'], 2)} ms</li>"
            for item in summary["most_affected_devices"]
        ]
    ) or "<li>No affected device summary available.</li>"

    root_causes_html = "".join(
        [
            f"<li><strong>{item['root_cause_hint']}</strong> "
            f"(workload: {item['workload_pattern']}, anomalies: {item['anomaly_count']}, "
            f"critical: {item['critical_count']})</li>"
            for item in summary["top_root_causes"]
        ]
    ) or "<li>No root-cause summary available.</li>"

    recommendations_html = "".join([f"<li>{rec}</li>" for rec in recommendations])

    workload_html = "".join(
        [f"<li><strong>{k}</strong>: {v} device(s)</li>" for k, v in summary["dominant_workloads"].items()]
    ) or "<li>No workload pattern summary available.</li>"

    return f"""
    <html>
    <head>
        <title>Storage Telemetry Investigation Summary</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; line-height: 1.5; }}
            h1, h2 {{ color: #222; }}
            .metric {{ margin-bottom: 6px; }}
        </style>
    </head>
    <body>
        <h1>Storage Telemetry Investigation Summary</h1>
        <p>Generated at: {datetime.utcnow().isoformat()} UTC</p>

        <h2>1. Run Overview</h2>
        <p class="metric">Total devices analyzed: <strong>{summary['total_devices']}</strong></p>
        <p class="metric">Total anomalies detected: <strong>{summary['total_anomalies']}</strong></p>
        <p class="metric">Critical anomalies: <strong>{summary['critical_anomalies']}</strong></p>
        <p class="metric">High severity anomalies: <strong>{summary['high_anomalies']}</strong></p>
        <p class="metric">Average device latency: <strong>{summary['avg_latency_ms']} ms</strong></p>
        <p class="metric">Average device utilization: <strong>{summary['avg_util_pct']}%</strong></p>

        <h2>2. Executive Summary</h2>
        <p>
            This report summarizes storage telemetry behavior across analyzed devices,
            with emphasis on anomaly frequency, severity distribution, workload patterns,
            and likely root-cause signals.
        </p>

        <h2>3. Dominant Workload Patterns</h2>
        <ul>{workload_html}</ul>

        <h2>4. Most Affected Devices</h2>
        <ul>{most_affected_html}</ul>

        <h2>5. Top Root-Cause Signals</h2>
        <ul>{root_causes_html}</ul>

        <h2>6. Recommended Next Checks</h2>
        <ul>{recommendations_html}</ul>
    </body>
    </html>
    """
