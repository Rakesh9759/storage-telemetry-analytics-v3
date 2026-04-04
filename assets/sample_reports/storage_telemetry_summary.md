# Storage Telemetry Investigation Summary

Generated at: 2026-04-04T17:46:36.004289 UTC

## 1. Run Overview

- Total devices analyzed: **5**
- Total anomalies detected: **2705**
- Critical anomalies: **650**
- High severity anomalies: **262**
- Average device latency: **22.7 ms**
- Average device utilization: **33.32%**

## 2. Executive Summary

This report summarizes storage telemetry behavior across analyzed devices, with emphasis on anomaly frequency, severity distribution, workload patterns, and likely root-cause signals.

## 3. Dominant Workload Patterns

- **balanced**: 4 device(s)
- **saturated**: 1 device(s)

## 4. Most Affected Devices

- **sdb** — anomalies: 1336, critical: 350, dominant workload: balanced, avg latency: 106.74 ms
- **nvme0n1** — anomalies: 229, critical: 121, dominant workload: balanced, avg latency: 0.61 ms
- **nvme1n1** — anomalies: 369, critical: 85, dominant workload: balanced, avg latency: 0.49 ms
- **dm-0** — anomalies: 503, critical: 48, dominant workload: saturated, avg latency: 4.15 ms
- **sda** — anomalies: 268, critical: 46, dominant workload: balanced, avg latency: 1.48 ms

## 5. Top Root-Cause Signals

- **Joint increase in latency and queue depth suggests pressure buildup** (workload: saturated, anomalies: 247, critical: 115)
- **Composite saturation signal indicates elevated device stress** (workload: saturated, anomalies: 198, critical: 56)
- **Joint increase in latency and queue depth suggests pressure buildup** (workload: latency_sensitive, anomalies: 104, critical: 53)
- **Queue buildup likely contributing to latency pressure** (workload: saturated, anomalies: 155, critical: 52)
- **Latency anomaly detected without clear saturation signal** (workload: balanced, anomalies: 190, critical: 50)

## 6. Recommended Next Checks

- Review critical anomaly windows first, prioritizing devices with repeated latency and saturation signals.
- Correlate high-utilization and queue-depth periods with latency spikes to validate saturation-driven degradation.
