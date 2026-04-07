import pandas as pd
import pytest
from storage_telemetry.exports.dashboard_views import build_device_overview_mart


def _curated(devices=("sda", "sdb"), rows_per_device=2) -> pd.DataFrame:
    records = []
    for dev in devices:
        for i in range(rows_per_device):
            records.append({
                "device": dev,
                "total_iops": 10.0 * (i + 1),
                "total_throughput_mb_s": 1.0 * (i + 1),
                "avg_latency_ms": 2.0 + i,
                "util_pct": 50.0 + i * 5,
                "aqu_sz": 1.0 + i * 0.5,
                "workload_pattern": "balanced",
            })
    return pd.DataFrame(records)


def _anomalies(devices=("sda",), severity="critical") -> pd.DataFrame:
    return pd.DataFrame([{"device": d, "severity": severity} for d in devices])


def test_mart_has_device_column():
    out = build_device_overview_mart(_curated(), _anomalies())
    assert "device" in out.columns


def test_mart_one_row_per_device():
    curated = _curated(devices=["sda", "sdb", "sdc"])
    anomalies = _anomalies(devices=["sda"])
    out = build_device_overview_mart(curated, anomalies)
    assert len(out) == 3, "Mart should have exactly one row per device"
    assert set(out["device"]) == {"sda", "sdb", "sdc"}


def test_mart_aggregates_metrics():
    """Aggregated avg_latency_ms should equal the mean of the input rows."""
    curated = _curated(devices=["sda"], rows_per_device=4)
    out = build_device_overview_mart(curated, pd.DataFrame(columns=["device", "severity"]))
    expected_latency = curated["avg_latency_ms"].mean()
    assert abs(out.loc[out["device"] == "sda", "avg_latency_ms"].iloc[0] - expected_latency) < 1e-6


def test_mart_device_with_no_anomalies():
    """Devices absent from the anomaly frame should still appear in the mart."""
    curated = _curated(devices=["sda", "sdb"])
    anomalies = _anomalies(devices=["sda"])  # sdb has no anomalies
    out = build_device_overview_mart(curated, anomalies)
    assert "sdb" in out["device"].values


def test_mart_empty_anomalies():
    out = build_device_overview_mart(_curated(), pd.DataFrame(columns=["device", "severity"]))
    assert not out.empty
    assert "device" in out.columns