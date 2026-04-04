from pathlib import Path
import pandas as pd

from storage_telemetry.storage.repository import read_table
from storage_telemetry.reporting.summary_builder import build_report_summary
from storage_telemetry.reporting.recommendations import generate_recommendations
from storage_telemetry.reporting.markdown_report import render_markdown_report
from storage_telemetry.reporting.html_report import render_html_report
from storage_telemetry.reporting.quality_checks import validate_report_inputs


def build_reports():
    device_overview = read_table("mart_tableau_device_overview")
    anomaly_timeline = read_table("mart_tableau_anomaly_timeline")
    root_cause_summary = read_table("mart_tableau_root_cause_summary")

    validate_report_inputs(device_overview, anomaly_timeline, root_cause_summary)

    summary = build_report_summary(device_overview, anomaly_timeline, root_cause_summary)
    recommendations = generate_recommendations(summary)

    markdown = render_markdown_report(summary, recommendations)
    html = render_html_report(summary, recommendations)

    output_dir = Path("assets/sample_reports")
    output_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / "storage_telemetry_summary.md").write_text(markdown, encoding="utf-8")
    (output_dir / "storage_telemetry_summary.html").write_text(html, encoding="utf-8")

    print("Reports generated successfully.")
