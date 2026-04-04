import argparse
from storage_telemetry.storage.init_db import init_db
from storage_telemetry.core.logging_utils import setup_logging
from storage_telemetry.ingestion.batch_ingest import run_batch_ingestion
from storage_telemetry.transforms.build_curated_metrics import build_curated_metrics
from storage_telemetry.detection.build_anomaly_events import build_anomaly_events
from storage_telemetry.exports.tableau_extracts import export_dashboard_datasets
from storage_telemetry.reporting.build_report import build_reports
from storage_telemetry.transforms.build_timeseries import build_timeseries
from storage_telemetry.storage.repository import read_table


def main():
    setup_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True)
    parser.add_argument("--file", help="Path to iostat file")

    args = parser.parse_args()

    if args.mode == "ingest":
        if not args.file:
            raise ValueError("Provide --file for ingest mode")
        run_batch_ingestion(args.file)

    elif args.mode == "curate":
        build_curated_metrics()

    elif args.mode == "detect":
        build_anomaly_events()

    elif args.mode == "export":
        export_dashboard_datasets()

    elif args.mode == "timeseries":
        curated_df = read_table("curated_device_metrics")
        build_timeseries(curated_df)

    elif args.mode == "report":
        build_reports()

    elif args.mode == "init-db":
        init_db()

    else:
        raise ValueError(f"Unknown mode: {args.mode}")

if __name__ == "__main__":
    main()
