import numpy as np
import pandas as pd


def derive_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["timestamp"] = pd.to_datetime(out["timestamp"])

    out["total_iops"] = out["r_s"] + out["w_s"]
    out["total_throughput_mb_s"] = out["rmb_s"] + out["wmb_s"]

    out["avg_latency_ms"] = np.where(
        (out["r_s"] + out["w_s"]) > 0,
        ((out["r_await"] * out["r_s"]) + (out["w_await"] * out["w_s"])) / (out["r_s"] + out["w_s"]),
        0.0
    )

    out["read_ratio"] = np.where(
        out["total_iops"] > 0,
        out["r_s"] / out["total_iops"],
        0.0
    )

    out["write_ratio"] = np.where(
        out["total_iops"] > 0,
        out["w_s"] / out["total_iops"],
        0.0
    )

    out["avg_request_size_kb"] = np.where(
        out["total_iops"] > 0,
        (out["total_throughput_mb_s"] * 1024) / out["total_iops"],
        0.0
    )

    out["saturation_score"] = (
        (out["util_pct"] * 0.5) +
        (out["aqu_sz"] * 30) +
        (out["avg_latency_ms"] * 2)
    )

    out["io_intensity"] = out["total_iops"] * out["total_throughput_mb_s"]
    out["latency_pressure"] = out["avg_latency_ms"] * (out["aqu_sz"] + 1)

    out["hour_of_day"] = out["timestamp"].dt.hour
    out["day_of_week"] = out["timestamp"].dt.dayofweek

    return out
