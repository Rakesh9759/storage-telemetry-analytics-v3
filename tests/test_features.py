import numpy as np
import pandas as pd
from storage_telemetry.transforms.derive_features import derive_features


def _base_row(**overrides) -> pd.DataFrame:
    row = {
        "device": "sda",
        "timestamp": "2025-01-01T00:00:00",
        "r_s": 10.0,
        "w_s": 5.0,
        "rmb_s": 1.0,
        "wmb_s": 1.0,
        "r_await": 2.0,
        "w_await": 4.0,
        "aqu_sz": 1.0,
        "util_pct": 50.0,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_total_iops():
    out = derive_features(_base_row(r_s=10, w_s=5))
    assert out["total_iops"].iloc[0] == 15.0


def test_total_throughput():
    out = derive_features(_base_row(rmb_s=2.0, wmb_s=3.0))
    assert out["total_throughput_mb_s"].iloc[0] == 5.0


def test_avg_latency_weighted():
    # weighted: (r_await * r_s + w_await * w_s) / (r_s + w_s) = (2*10 + 4*5) / 15 = 40/15
    out = derive_features(_base_row(r_s=10, w_s=5, r_await=2.0, w_await=4.0))
    expected = (2.0 * 10 + 4.0 * 5) / 15
    assert abs(out["avg_latency_ms"].iloc[0] - expected) < 1e-9


def test_avg_latency_zero_iops():
    out = derive_features(_base_row(r_s=0, w_s=0))
    assert out["avg_latency_ms"].iloc[0] == 0.0


def test_read_write_ratios_sum_to_one():
    out = derive_features(_base_row(r_s=10, w_s=10))
    assert abs(out["read_ratio"].iloc[0] + out["write_ratio"].iloc[0] - 1.0) < 1e-9


def test_read_write_ratios_zero_iops():
    out = derive_features(_base_row(r_s=0, w_s=0))
    assert out["read_ratio"].iloc[0] == 0.0
    assert out["write_ratio"].iloc[0] == 0.0


def test_avg_request_size_kb():
    # (total_throughput_mb_s * 1024) / total_iops = (2 * 1024) / 20 = 102.4
    out = derive_features(_base_row(r_s=10, w_s=10, rmb_s=1.0, wmb_s=1.0))
    expected = (2.0 * 1024) / 20
    assert abs(out["avg_request_size_kb"].iloc[0] - expected) < 1e-6


def test_saturation_score_is_positive():
    out = derive_features(_base_row(util_pct=80, aqu_sz=2, r_await=5, w_await=5))
    assert out["saturation_score"].iloc[0] > 0


def test_temporal_columns_extracted():
    out = derive_features(_base_row(timestamp="2025-06-15T14:30:00"))
    assert out["hour_of_day"].iloc[0] == 14
    assert out["day_of_week"].iloc[0] == pd.Timestamp("2025-06-15").dayofweek