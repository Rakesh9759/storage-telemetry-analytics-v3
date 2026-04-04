from storage_telemetry.ingestion.iostat_parser import parse_iostat_file


def test_parse_iostat_file():
    df = parse_iostat_file("data/raw/sample_iostat.log")

    assert not df.empty
    assert "device" in df.columns
    assert "r_s" in df.columns
