import pandas as pd
from storage_telemetry.transforms.derive_features import derive_features


def test_feature_derivation():
    df = pd.DataFrame({
        "device": ["sda"],
        "timestamp": ["2025-01-01T00:00:00"],
        "r_s": [10],
        "w_s": [5],
        "rmb_s": [1],
        "wmb_s": [1],
        "r_await": [2],
        "w_await": [2],
        "aqu_sz": [1],
        "util_pct": [50],
    })

    out = derive_features(df)

    assert "total_iops" in out.columns
    assert out["total_iops"].iloc[0] == 15
