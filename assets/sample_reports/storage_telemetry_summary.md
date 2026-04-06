# Storage Telemetry Investigation Summary

Generated at: 2026-04-06T00:02:43.217620 UTC

## 1. Run Overview

- Total devices analyzed: **5**
- Total anomalies detected: **2890**
- Critical anomalies: **169**
- High severity anomalies: **179**
- Average device latency: **52.57 ms**
- Average device utilization: **42.86%**

## 2. Executive Summary

This report summarizes storage telemetry behavior across analyzed devices, with emphasis on anomaly frequency, severity distribution, workload patterns, and likely root-cause signals.

## 3. Dominant Workload Patterns

- **balanced**: 4 device(s)
- **latency_sensitive**: 1 device(s)

## 4. Most Affected Devices

- **nvme1n1** — anomalies: 500, critical: 54, dominant workload: balanced, avg latency: 0.32 ms
- **sda** — anomalies: 589, critical: 51, dominant workload: balanced, avg latency: 4.38 ms
- **dm-0** — anomalies: 500, critical: 28, dominant workload: balanced, avg latency: 1.1 ms
- **sdb** — anomalies: 740, critical: 19, dominant workload: latency_sensitive, avg latency: 255.61 ms
- **nvme0n1** — anomalies: 561, critical: 17, dominant workload: balanced, avg latency: 1.43 ms

## 5. Top Root-Cause Signals

- **avg_latency_ms deviated from device baseline** (workload: latency_sensitive, anomalies: 346, critical: 30)
- **CPU I/O wait pressure indicates backend storage bottleneck** (workload: latency_sensitive, anomalies: 37, critical: 17)
- **total_iops deviated from device baseline** (workload: burst_io, anomalies: 96, critical: 16)
- **total_throughput_mb_s deviated from device baseline** (workload: burst_io, anomalies: 27, critical: 16)
- **queue_efficiency deviated from device baseline** (workload: latency_sensitive, anomalies: 164, critical: 13)

## 6. Recommended Next Checks

- Review critical anomaly windows first, prioritizing devices with repeated latency and saturation signals.
