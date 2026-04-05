# Storage Telemetry Investigation Summary

Generated at: 2026-04-04T22:28:46.556053 UTC

## 1. Run Overview

- Total devices analyzed: **5**
- Total anomalies detected: **2731**
- Critical anomalies: **675**
- High severity anomalies: **269**
- Average device latency: **68.02 ms**
- Average device utilization: **37.63%**

## 2. Executive Summary

This report summarizes storage telemetry behavior across analyzed devices, with emphasis on anomaly frequency, severity distribution, workload patterns, and likely root-cause signals.

## 3. Dominant Workload Patterns

- **balanced**: 3 device(s)
- **burst_io**: 1 device(s)
- **saturated**: 1 device(s)

## 4. Most Affected Devices

- **sdb** — anomalies: 1437, critical: 350, dominant workload: saturated, avg latency: 335.89 ms
- **nvme0n1** — anomalies: 209, critical: 101, dominant workload: balanced, avg latency: 0.61 ms
- **dm-0** — anomalies: 466, critical: 91, dominant workload: balanced, avg latency: 1.31 ms
- **sda** — anomalies: 298, critical: 74, dominant workload: balanced, avg latency: 1.57 ms
- **nvme1n1** — anomalies: 321, critical: 59, dominant workload: burst_io, avg latency: 0.73 ms

## 5. Top Root-Cause Signals

- **Joint increase in latency and queue depth suggests pressure buildup** (workload: saturated, anomalies: 365, critical: 152)
- **Composite saturation signal indicates elevated device stress** (workload: saturated, anomalies: 331, critical: 122)
- **Latency anomaly detected without clear saturation signal** (workload: saturated, anomalies: 115, critical: 65)
- **Latency spike likely driven by saturation and queue buildup** (workload: saturated, anomalies: 93, critical: 65)
- **Queue buildup likely contributing to latency pressure** (workload: saturated, anomalies: 154, critical: 49)

## 6. Recommended Next Checks

- Review critical anomaly windows first, prioritizing devices with repeated latency and saturation signals.
- Correlate high-utilization and queue-depth periods with latency spikes to validate saturation-driven degradation.
