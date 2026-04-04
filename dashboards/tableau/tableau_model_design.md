# Tableau Model Design

## Primary datasets

### 1. mart_tableau_device_overview
Use for:
- device comparison
- executive summary KPIs
- latency percentile comparisons
- anomaly burden by device

### 2. mart_tableau_anomaly_timeline
Use for:
- anomaly event drilldown
- severity timeline
- root-cause analysis
- metric-level anomaly inspection

### 3. mart_tableau_root_cause_summary
Use for:
- explanation summaries
- workload vs root-cause distributions
- analyst storytelling

## Recommended dashboards

### Executive Storage Health Overview
- total anomaly count
- critical anomalies
- average device latency
- p95/p99 latency by device
- dominant workload pattern by device

### Anomaly Timeline Investigation
- anomaly events over time
- severity filter
- device filter
- root-cause hint filter
- workload pattern filter

### Root Cause & Workload Summary
- top root-cause hints
- anomaly count by workload pattern
- critical events by device
