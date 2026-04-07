import argparse
from storage_telemetry.storage.init_db import init_db
from storage_telemetry.core.logging_utils import setup_logging


def main():
    setup_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True)

    args = parser.parse_args()

    if args.mode == "report":
        from storage_telemetry.reporting.build_report import build_reports
        build_reports()

    elif args.mode == "init-db":
        init_db()

    else:
        raise ValueError(f"Unknown mode: {args.mode}. Supported: report, init-db")

if __name__ == "__main__":
    main()
