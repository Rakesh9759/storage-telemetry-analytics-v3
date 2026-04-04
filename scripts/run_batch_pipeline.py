"""
Full batch pipeline runner.
Executes all pipeline stages in sequence: ingest → curate → detect → export → timeseries → report.
"""

from storage_telemetry.cli import main as cli_main
import sys


PIPELINE_STAGES = [
    ["--mode", "ingest", "--file", "data/raw/generated_iostat.csv"],
    ["--mode", "curate"],
    ["--mode", "detect"],
    ["--mode", "export"],
    ["--mode", "timeseries"],
    ["--mode", "report"],
]


def run_pipeline():
    for stage_args in PIPELINE_STAGES:
        print(f"\n>>> Running: {' '.join(stage_args)}")
        sys.argv = ["cli"] + stage_args
        cli_main()
    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
