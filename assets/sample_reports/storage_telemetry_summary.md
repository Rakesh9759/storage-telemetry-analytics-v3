# Storage Telemetry Investigation Summary

Generated at: 2026-04-07T04:09:20.668244 UTC

## 1. Run Overview

- Total devices analyzed: **5**
- Total anomalies detected: **2804**
- Critical anomalies: **272**
- High severity anomalies: **103**
- Average device latency: **22.23 ms**
- Average device utilization: **28.21%**

## 2. Executive Summary

This report summarizes storage telemetry behavior across analyzed devices, with emphasis on anomaly frequency, severity distribution, workload patterns, and likely root-cause signals.

## 3. Dominant Workload Patterns

- **balanced**: 4 device(s)
- **latency_sensitive**: 1 device(s)

## 4. Most Affected Devices

- **nvme0n1** — anomalies: 453, critical: 81, dominant workload: balanced, avg latency: 0.47 ms
- **sda** — anomalies: 622, critical: 61, dominant workload: balanced, avg latency: 3.47 ms
- **dm-0** — anomalies: 518, critical: 48, dominant workload: balanced, avg latency: 1.3 ms
- **nvme1n1** — anomalies: 452, critical: 42, dominant workload: balanced, avg latency: 0.3 ms
- **sdb** — anomalies: 759, critical: 40, dominant workload: latency_sensitive, avg latency: 105.59 ms

## 5. Top Root-Cause Signals

- **Anomalous behavior detected** (workload: latency_sensitive, anomalies: 288, critical: 47)
- **Latency anomaly detected without clear saturation signal** (workload: latency_sensitive, anomalies: 290, critical: 36)
- **Composite saturation signal indicates elevated device stress** (workload: saturated, anomalies: 157, critical: 31)
- **Anomalous behavior detected** (workload: balanced, anomalies: 709, critical: 29)
- **Composite saturation signal indicates elevated device stress** (workload: latency_sensitive, anomalies: 166, critical: 29)

## 6. Recommended Next Checks

- Review critical anomaly windows first, prioritizing devices with repeated latency and saturation signals.
- Correlate high-utilization and queue-depth periods with latency spikes to validate saturation-driven degradation.
