# Storage Telemetry Investigation Summary

Generated at: 2026-04-06T00:39:18.699367 UTC

## 1. Run Overview

- Total devices analyzed: **5**
- Total anomalies detected: **6332**
- Critical anomalies: **313**
- High severity anomalies: **369**
- Average device latency: **40.77 ms**
- Average device utilization: **38.67%**

## 2. Executive Summary

This report summarizes storage telemetry behavior across analyzed devices, with emphasis on anomaly frequency, severity distribution, workload patterns, and likely root-cause signals.

## 3. Dominant Workload Patterns

- **balanced**: 4 device(s)
- **latency_sensitive**: 1 device(s)

## 4. Most Affected Devices

- **sda** — anomalies: 1345, critical: 85, dominant workload: balanced, avg latency: 2.91 ms
- **dm-0** — anomalies: 975, critical: 79, dominant workload: balanced, avg latency: 1.12 ms
- **nvme1n1** — anomalies: 1202, critical: 63, dominant workload: balanced, avg latency: 0.65 ms
- **nvme0n1** — anomalies: 1099, critical: 53, dominant workload: balanced, avg latency: 1.27 ms
- **sdb** — anomalies: 1711, critical: 33, dominant workload: latency_sensitive, avg latency: 197.9 ms

## 5. Top Root-Cause Signals

- **avg_latency_ms deviated from device baseline** (workload: latency_sensitive, anomalies: 791, critical: 97)
- **CPU I/O wait pressure indicates backend storage bottleneck** (workload: latency_sensitive, anomalies: 270, critical: 35)
- **queue_efficiency deviated from device baseline** (workload: latency_sensitive, anomalies: 397, critical: 34)
- **Device saturation with queue buildup** (workload: latency_sensitive, anomalies: 94, critical: 19)
- **saturation_score deviated from device baseline** (workload: latency_sensitive, anomalies: 257, critical: 16)

## 6. Recommended Next Checks

- Review critical anomaly windows first, prioritizing devices with repeated latency and saturation signals.
- Correlate high-utilization and queue-depth periods with latency spikes to validate saturation-driven degradation.
