import pandas as pd
from storage_telemetry.exports.dashboard_views import build_device_overview_mart


def test_device_overview_mart():
    curated = pd.DataFrame({
        "device": ["sda", "sda"],
        "total_iops": [10, 20],
        "total_throughput_mb_s": [1, 2],
        "avg_latency_ms": [2, 3],
        "util_pct": [50, 60],
        "aqu_sz": [1, 2],
        "workload_pattern": ["balanced", "balanced"]
    })

    anomaly = pd.DataFrame({
        "device": ["sda"],
        "severity": ["critical"]
    })

    out = build_device_overview_mart(curated, anomaly)

    assert "device" in out.columns
