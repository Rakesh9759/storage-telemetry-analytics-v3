# Storage Telemetry Investigation Summary

Generated at: 2026-04-08T19:20:59.244243 UTC

## 1. Run Overview

- Total devices analyzed: **5**
- Total anomalies detected: **13703**
- Critical anomalies: **1141**
- High severity anomalies: **563**
- Average device latency: **46.73 ms**
- Average device utilization: **30.8%**

## 2. Executive Summary

This report summarizes storage telemetry behavior across analyzed devices, with emphasis on anomaly frequency, severity distribution, workload patterns, and likely root-cause signals.

## 3. Dominant Workload Patterns

- **balanced**: 4 device(s)
- **latency_sensitive**: 1 device(s)

## 4. Most Affected Devices

- **sdb** — anomalies: 4208, critical: 265, dominant workload: latency_sensitive, avg latency: 229.27 ms
- **sda** — anomalies: 2446, critical: 265, dominant workload: balanced, avg latency: 1.92 ms
- **dm-0** — anomalies: 2584, critical: 213, dominant workload: balanced, avg latency: 1.52 ms
- **nvme0n1** — anomalies: 2084, critical: 208, dominant workload: balanced, avg latency: 0.51 ms
- **nvme1n1** — anomalies: 2381, critical: 190, dominant workload: balanced, avg latency: 0.41 ms

## 5. Top Root-Cause Signals

- **Anomalous behavior detected** (workload: latency_sensitive, anomalies: 369, critical: 71)
- **Anomalous behavior detected** (workload: latency_sensitive, anomalies: 284, critical: 54)
- **Anomalous behavior detected** (workload: latency_sensitive, anomalies: 367, critical: 49)
- **Anomalous behavior detected** (workload: latency_sensitive, anomalies: 288, critical: 47)
- **Anomalous behavior detected** (workload: latency_sensitive, anomalies: 306, critical: 45)

## 6. Recommended Next Checks

- Review critical anomaly windows first, prioritizing devices with repeated latency and saturation signals.
