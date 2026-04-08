import argparse
import os

from storage_telemetry.storage.init_db import init_db
from storage_telemetry.core.logging_utils import setup_logging
from storage_telemetry.analytics.sql_assistant import generate_sql, run_query, summarize_results


def main():
    setup_logging()

    parser = argparse.ArgumentParser(description="Storage Telemetry CLI")
    parser.add_argument("--mode", required=True,
                        choices=["report", "init-db", "ask-sql", "ask-data"],
                        help="Operation mode")
    parser.add_argument("--question", default=None,
                        help="Natural language question (required for ask-sql and ask-data)")
    parser.add_argument("--model", default="openai/gpt-4.1",
                        help="GitHub Models model ID (default: openai/gpt-4.1)")
    parser.add_argument("--api-key", default=os.getenv("LLM_API_KEY"),
                        help="GitHub PAT with models:read permission (or set LLM_API_KEY env var)")
    parser.add_argument("--api-endpoint", default="https://models.github.ai/inference/chat/completions",
                        help="GitHub Models API endpoint")

    args = parser.parse_args()

    if args.mode == "report":
        from storage_telemetry.reporting.build_report import build_reports
        build_reports()

    elif args.mode == "init-db":
        init_db()

    elif args.mode == "ask-sql":
        if not args.question:
            raise ValueError("--question is required for ask-sql")
        try:
            sql = generate_sql(args.question, model=args.model, api_key=args.api_key, api_endpoint=args.api_endpoint)
            print(sql)
        except RuntimeError as exc:
            print(f"ERROR: {exc}")

    elif args.mode == "ask-data":
        if not args.question:
            raise ValueError("--question is required for ask-data")
        try:
            sql = generate_sql(args.question, model=args.model, api_key=args.api_key, api_endpoint=args.api_endpoint)
        except RuntimeError as exc:
            print(f"ERROR: {exc}")
            return

        if sql == "CANNOT_ANSWER":
            print("CANNOT_ANSWER: this question cannot be answered from the available data.")
            return

        results = run_query(sql)

        print("SQL:")
        print(sql)
        print("\nRESULTS:")
        print(results.head(50).to_string(index=False) if not results.empty else "<no rows>")

        try:
            summary = summarize_results(
                args.question, results,
                model=args.model,
                api_key=args.api_key,
                api_endpoint=args.api_endpoint,
            )
            print("\nSUMMARY:")
            print(summary)
        except RuntimeError as exc:
            print(f"\nSUMMARY: (unavailable - {exc})")


if __name__ == "__main__":
    main()