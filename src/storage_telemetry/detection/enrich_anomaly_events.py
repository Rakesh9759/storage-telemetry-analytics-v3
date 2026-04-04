import pandas as pd

from storage_telemetry.detection.root_cause_rules import derive_root_cause_hint


CONTEXT_COLUMNS = [
    "device",
    "timestamp",
    "util_pct",
    "aqu_sz",
    "avg_latency_ms",
    "read_ratio",
    "write_ratio",
    "avg_request_size_kb",
    "workload_pattern",
    "total_iops",
    "total_throughput_mb_s",
    "saturation_score",
    "latency_pressure",
]


def enrich_anomaly_events(
    anomaly_df: pd.DataFrame,
    curated_df: pd.DataFrame
) -> pd.DataFrame:
    curated_context = curated_df[CONTEXT_COLUMNS].copy()
    curated_context["timestamp"] = pd.to_datetime(curated_context["timestamp"])
    anomaly_df = anomaly_df.copy()
    anomaly_df["timestamp"] = pd.to_datetime(anomaly_df["timestamp"])

    enriched = anomaly_df.merge(
        curated_context,
        on=["device", "timestamp"],
        how="left"
    )

    enriched["root_cause_hint"] = enriched.apply(derive_root_cause_hint, axis=1)
    return enriched
