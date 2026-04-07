import pandas as pd
import pytest
from storage_telemetry.ingestion.iostat_parser import parse_iostat_file


SAMPLE_LOG = "data/raw/sample_iostat.log"

EXPECTED_COLUMNS = {"device", "timestamp", "r_s", "w_s", "rmb_s", "wmb_s",
                    "r_await", "w_await", "aqu_sz", "util_pct"}


def test_parse_returns_nonempty_dataframe():
    df = parse_iostat_file(SAMPLE_LOG)
    assert not df.empty


def test_parse_has_required_columns():
    df = parse_iostat_file(SAMPLE_LOG)
    missing = EXPECTED_COLUMNS - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


def test_parse_numeric_metric_columns():
    df = parse_iostat_file(SAMPLE_LOG)
    numeric_cols = ["r_s", "w_s", "rmb_s", "wmb_s", "r_await", "w_await", "aqu_sz", "util_pct"]
    for col in numeric_cols:
        assert pd.api.types.is_numeric_dtype(df[col]), f"Column '{col}' should be numeric"


def test_parse_no_null_device_or_timestamp():
    df = parse_iostat_file(SAMPLE_LOG)
    assert df["device"].notna().all(), "device column must not contain nulls"
    assert df["timestamp"].notna().all(), "timestamp column must not contain nulls"


def test_parse_util_pct_in_valid_range():
    df = parse_iostat_file(SAMPLE_LOG)
    assert (df["util_pct"] >= 0).all(), "util_pct must be >= 0"
    assert (df["util_pct"] <= 100).all(), "util_pct must be <= 100"


def test_parse_non_negative_metrics():
    df = parse_iostat_file(SAMPLE_LOG)
    for col in ["r_s", "w_s", "rmb_s", "wmb_s", "aqu_sz"]:
        assert (df[col] >= 0).all(), f"Column '{col}' must not be negative"