"""Build dashboard marts from curated metrics and anomaly events."""

from storage_telemetry.exports.tableau_extracts import export_dashboard_datasets


def main() -> None:
    """Generate Tableau/Grafana marts and export datasets."""
    export_dashboard_datasets()


if __name__ == "__main__":
    main()
