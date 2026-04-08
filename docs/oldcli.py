# import argparse

# from storage_telemetry.storage.init_db import init_db
# from storage_telemetry.core.logging_utils import setup_logging
# from storage_telemetry.analytics.sql_assistant import generate_sql, run_query, summarize_results


# def main():
#     setup_logging()

#     parser = argparse.ArgumentParser()
#     parser.add_argument("--mode", required=True)
#     parser.add_argument("--question", default=None)
#     parser.add_argument("--use-llm", action="store_true")
#     parser.add_argument("--llm-model", default="llama3.1:8b")
#     parser.add_argument("--llm-host", default="http://localhost:11434")

#     args = parser.parse_args()

#     if args.mode == "report":
#         from storage_telemetry.reporting.build_report import build_reports
#         build_reports()

#     elif args.mode == "init-db":
#         init_db()

#     elif args.mode == "ask-sql":
#         if not args.question:
#             raise ValueError("--question is required for mode ask-sql")
#         sql = generate_sql(
#             args.question,
#             use_llm=args.use_llm,
#             llm_model=args.llm_model,
#             llm_host=args.llm_host,
#         )
#         print(sql)

#     elif args.mode == "ask-data":
#         if not args.question:
#             raise ValueError("--question is required for mode ask-data")

#         sql = generate_sql(
#             args.question,
#             use_llm=args.use_llm,
#             llm_model=args.llm_model,
#             llm_host=args.llm_host,
#         )

#         if sql == "CANNOT_ANSWER":
#             print("CANNOT_ANSWER: unable to translate this question into a query.")
#             return

#         results = run_query(sql)
#         summary = summarize_results(
#             args.question,
#             results,
#             use_llm=args.use_llm,
#             llm_model=args.llm_model,
#             llm_host=args.llm_host,
#         )

#         print("SQL:")
#         print(sql)
#         print("\nRESULTS:")
#         if results.empty:
#             print("<no rows>")
#         else:
#             print(results.head(50).to_string(index=False))
#         print("\nSUMMARY:")
#         print(summary)

#     else:
#         raise ValueError(
#             f"Unknown mode: {args.mode}. "
#             "Supported: report, init-db, ask-sql, ask-data"
#         )


# if __name__ == "__main__":
#     main()
