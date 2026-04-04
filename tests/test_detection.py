import pandas as pd
from storage_telemetry.detection.rolling_zscore import detect_rolling_zscore


def test_zscore_detection_runs():
    df = pd.DataFrame({
        "device": ["sda"] * 10,
        "timestamp": pd.date_range("2025-01-01", periods=10),
        "avg_latency_ms": [1,1,1,1,1,10,1,1,1,1],
        "avg_latency_ms_rolling_mean": [1]*10,
        "avg_latency_ms_rolling_std": [0.5]*10,
    })

    events = detect_rolling_zscore(df, ["avg_latency_ms"], threshold=3)

    assert isinstance(events, pd.DataFrame)
