# Reporting Strategy

## Goal
Generate concise investigation summaries that translate telemetry anomalies into readable operational findings.

The reporting layer is fed from Postgres mart tables built after Spark curation and anomaly scoring:

- `mart_tableau_device_overview`
- `mart_tableau_anomaly_timeline`
- `mart_tableau_root_cause_summary`
- `v_grafana_device_health`


## Core sections
- run overview
- executive summary
- dominant workload patterns
- most affected devices
- top root-cause signals
- recommended next checks

## Output Artifacts

- Markdown summary: `assets/sample_reports/storage_telemetry_summary.md`
- HTML summary: `assets/sample_reports/storage_telemetry_summary.html`
