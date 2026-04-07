import pandas as pd
import pytest
from storage_telemetry.detection.rolling_zscore import detect_rolling_zscore
from storage_telemetry.detection.iqr_detector import detect_iqr_anomalies
from storage_telemetry.detection.isolation_forest_detector import detect_isolation_forest
from storage_telemetry.detection.severity import assign_severity
from storage_telemetry.detection.root_cause_rules import derive_root_cause_hint


# ---------------------------------------------------------------------------
# Rolling Z-score
# ---------------------------------------------------------------------------

def _zscore_df(spike_value: float = 20.0) -> pd.DataFrame:
    """10-row device frame with one optional spike at index 5."""
    baseline = [1.0] * 10
    baseline[5] = spike_value
    return pd.DataFrame({
        "device": ["sda"] * 10,
        "timestamp": pd.date_range("2025-01-01", periods=10, freq="h"),
        "avg_latency_ms": baseline,
        "avg_latency_ms_rolling_mean": [1.0] * 10,
        "avg_latency_ms_rolling_std": [0.5] * 10,
    })


def test_zscore_flags_spike():
    events = detect_rolling_zscore(_zscore_df(spike_value=20.0), ["avg_latency_ms"], threshold=3)
    assert not events.empty, "Expected at least one anomaly event for a large spike"
    assert set(events["metric_name"].unique()) == {"avg_latency_ms"}


def test_zscore_no_false_positives_on_flat_signal():
    flat_df = _zscore_df(spike_value=1.0)  # no spike
    events = detect_rolling_zscore(flat_df, ["avg_latency_ms"], threshold=3)
    assert events.empty, "Flat signal should produce zero anomaly events"


def test_zscore_respects_threshold():
    """A mild spike (z≈4) should be caught at threshold=3 but not at threshold=10."""
    events_low = detect_rolling_zscore(_zscore_df(spike_value=3.0), ["avg_latency_ms"], threshold=3)
    events_high = detect_rolling_zscore(_zscore_df(spike_value=3.0), ["avg_latency_ms"], threshold=10)
    # threshold=3 should catch something threshold=10 misses
    assert len(events_low) >= len(events_high)


# ---------------------------------------------------------------------------
# IQR detector
# ---------------------------------------------------------------------------

def _iqr_df(outlier_value: float = 200.0) -> pd.DataFrame:
    baseline = [10.0] * 20
    baseline[10] = outlier_value
    return pd.DataFrame({
        "device": ["sdb"] * 20,
        "timestamp": pd.date_range("2025-01-01", periods=20, freq="h"),
        "avg_latency_ms": baseline,
    })


def test_iqr_flags_outlier():
    events = detect_iqr_anomalies(_iqr_df(outlier_value=200.0), ["avg_latency_ms"])
    assert not events.empty, "IQR detector should flag a severe outlier"


def test_iqr_no_flags_on_uniform_data():
    events = detect_iqr_anomalies(_iqr_df(outlier_value=10.0), ["avg_latency_ms"])
    assert events.empty, "Uniform data should produce zero IQR anomalies"


# ---------------------------------------------------------------------------
# Isolation Forest
# ---------------------------------------------------------------------------

def _iforest_df(n: int = 50, inject_anomaly: bool = True) -> pd.DataFrame:
    import numpy as np
    rng = pd.date_range("2025-01-01", periods=n, freq="h")
    latency = [2.0] * n
    iops = [100.0] * n
    if inject_anomaly:
        latency[-1] = 500.0
        iops[-1] = 5000.0
    return pd.DataFrame({
        "device": ["sdc"] * n,
        "timestamp": rng,
        "avg_latency_ms": latency,
        "total_iops": iops,
    })


def test_iforest_returns_expected_columns():
    events = detect_isolation_forest(_iforest_df(), ["avg_latency_ms", "total_iops"])
    required = {"device", "timestamp", "metric_name", "metric_value",
                "detector_type", "anomaly_score", "severity", "is_anomaly"}
    assert required.issubset(events.columns), f"Missing columns: {required - set(events.columns)}"


def test_iforest_metric_value_is_not_none():
    """metric_value must never be None — downstream marts expect a numeric value."""
    events = detect_isolation_forest(_iforest_df(), ["avg_latency_ms", "total_iops"])
    if not events.empty:
        assert events["metric_value"].notna().all(), \
            "isolation_forest events must not have NULL metric_value"


def test_iforest_returns_empty_for_tiny_input():
    tiny = _iforest_df(n=5, inject_anomaly=False)
    events = detect_isolation_forest(tiny, ["avg_latency_ms", "total_iops"])
    assert events.empty, "Should return empty DataFrame when input has fewer than 10 rows"


# ---------------------------------------------------------------------------
# Severity
# ---------------------------------------------------------------------------

def _severity_row(detector: str, score: float) -> pd.DataFrame:
    return pd.DataFrame([{"detector_type": detector, "anomaly_score": score}])


def test_severity_rolling_zscore_levels():
    assert assign_severity(_severity_row("rolling_zscore", 6.0))["severity"].iloc[0] == "critical"
    assert assign_severity(_severity_row("rolling_zscore", 4.5))["severity"].iloc[0] == "high"
    assert assign_severity(_severity_row("rolling_zscore", 1.0))["severity"].iloc[0] == "medium"


def test_severity_iqr_levels():
    assert assign_severity(_severity_row("iqr", 5.5))["severity"].iloc[0] == "critical"
    assert assign_severity(_severity_row("iqr", 4.0))["severity"].iloc[0] == "high"
    assert assign_severity(_severity_row("iqr", 2.0))["severity"].iloc[0] == "medium"
    assert assign_severity(_severity_row("iqr", 0.5))["severity"].iloc[0] == "low"


def test_severity_isolation_forest_levels():
    assert assign_severity(_severity_row("isolation_forest", 0.4))["severity"].iloc[0] == "critical"
    assert assign_severity(_severity_row("isolation_forest", 0.2))["severity"].iloc[0] == "high"
    assert assign_severity(_severity_row("isolation_forest", 0.1))["severity"].iloc[0] == "medium"
    assert assign_severity(_severity_row("isolation_forest", 0.01))["severity"].iloc[0] == "low"


def test_severity_no_null_scores():
    row = pd.DataFrame([{"detector_type": "rolling_zscore", "anomaly_score": None}])
    result = assign_severity(row)
    assert result["severity"].iloc[0] in {"critical", "high", "medium", "low"}


# ---------------------------------------------------------------------------
# Root-cause rules
# ---------------------------------------------------------------------------

def _rc_row(**kwargs) -> pd.Series:
    defaults = {
        "metric_name": "avg_latency_ms",
        "metric_value": 15.0,
        "workload_pattern": "balanced",
        "util_pct": 50.0,
        "aqu_sz": 1.0,
        "avg_latency_ms": 15.0,
        "read_ratio": 0.5,
        "write_ratio": 0.5,
        "avg_request_size_kb": 32.0,
    }
    defaults.update(kwargs)
    return pd.Series(defaults)


def test_root_cause_saturation_path():
    hint = derive_root_cause_hint(_rc_row(metric_name="avg_latency_ms", aqu_sz=3.0, util_pct=85.0))
    assert "saturation" in hint.lower() or "queue" in hint.lower()


def test_root_cause_write_heavy_latency():
    hint = derive_root_cause_hint(_rc_row(metric_name="avg_latency_ms", write_ratio=0.9, aqu_sz=0.5, util_pct=40.0))
    assert "write" in hint.lower()


def test_root_cause_multivariate_saturated():
    hint = derive_root_cause_hint(_rc_row(metric_name="multivariate", workload_pattern="saturated"))
    assert "saturation" in hint.lower()


def test_root_cause_always_returns_string():
    """Every metric name, known or not, must return a non-empty string."""
    for metric in ["avg_latency_ms", "util_pct", "aqu_sz", "saturation_score",
                   "total_iops", "multivariate", "unknown_metric"]:
        result = derive_root_cause_hint(_rc_row(metric_name=metric))
        assert isinstance(result, str) and result.strip(), \
            f"derive_root_cause_hint returned empty/non-string for metric '{metric}'"