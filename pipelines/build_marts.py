"""Build dashboard marts from curated metrics and anomaly events."""

import argparse

from storage_telemetry.exports.tableau_extracts import export_dashboard_datasets


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for mart build stage."""
    parser = argparse.ArgumentParser(description="Build dashboard marts")
    parser.add_argument("--ingest-run-id", default=None, help="Optional ingest run ID to process")
    return parser.parse_args()


def main() -> None:
    """Generate Tableau/Grafana marts and export datasets."""
    args = parse_args()
    export_dashboard_datasets(ingest_run_id=args.ingest_run_id)


if __name__ == "__main__":
    main()
