# """
# SQL Assistant for storage telemetry analytics.

# Architecture:
# - LLM-first: Ollama handles all arbitrary natural language -> SQL translation
# - Three-attempt generation: standard -> nudge -> chain-of-thought
# - Minimal rule-based fallback: only for when Ollama is unreachable
# - Safety validation: blocks any non-SELECT SQL before execution
# - Two-call pattern: SQL generation + result summarization
# """

# import json
# import re
# from urllib import error, request

# import pandas as pd
# from sqlalchemy import text

# from storage_telemetry.storage.db_connection import get_engine


# FORBIDDEN_KEYWORDS = ("insert", "update", "delete", "drop", "truncate", "alter", "create")

# # ---------------------------------------------------------------------------
# # System prompt — schema + rules + examples
# # ---------------------------------------------------------------------------

# SQL_SYSTEM_PROMPT = """
# You are a SQL assistant for a storage telemetry analytics platform.
# Translate natural language questions into valid PostgreSQL queries.
# Return ONLY the raw SQL query — no explanation, no markdown, no backticks, no "SQL:" prefix.

# DATABASE SCHEMA:

# -- Raw iostat metrics ingested per pipeline run. Each run has a unique ingest_run_id.
# CREATE TABLE raw_device_metrics (
#     device TEXT,
#     timestamp TIMESTAMP WITH TIME ZONE,
#     ingest_run_id TEXT,
#     r_s REAL, w_s REAL, rmb_s REAL, wmb_s REAL,
#     r_await REAL, w_await REAL, aqu_sz REAL, util_pct REAL,
#     rrqm_s REAL, wrqm_s REAL, rareq_sz REAL, wareq_sz REAL,
#     svctm REAL, iowait_pct REAL,
#     source_file TEXT
# );

# -- Feature-engineered metrics derived from raw, one row per device per timestamp per run.
# CREATE TABLE curated_device_metrics (
#     device TEXT,
#     timestamp TIMESTAMP WITH TIME ZONE,
#     ingest_run_id TEXT,
#     total_iops REAL, total_throughput_mb_s REAL,
#     avg_latency_ms REAL, read_ratio REAL, write_ratio REAL,
#     avg_request_size_kb REAL, saturation_score REAL,
#     latency_pressure REAL, merge_efficiency REAL,
#     queue_efficiency REAL, iowait_pressure REAL,
#     workload_pattern TEXT,
#     util_pct REAL, aqu_sz REAL,
#     hour_of_day INTEGER, day_of_week INTEGER
# );

# -- One row per anomaly event detected, per device per metric per run.
# CREATE TABLE anomaly_events (
#     device TEXT,
#     timestamp TIMESTAMP WITH TIME ZONE,
#     ingest_run_id TEXT,
#     metric_name TEXT,
#     metric_value REAL,
#     detector_type TEXT,
#     anomaly_score REAL,
#     severity TEXT,          -- values: 'low', 'medium', 'high', 'critical'
#     root_cause_hint TEXT,
#     workload_pattern TEXT,
#     util_pct REAL, aqu_sz REAL, avg_latency_ms REAL,
#     read_ratio REAL, write_ratio REAL,
#     avg_request_size_kb REAL, total_iops REAL,
#     total_throughput_mb_s REAL, saturation_score REAL,
#     latency_pressure REAL
# );

# -- Aggregated root-cause/workload summary used by reporting and dashboard storytelling.
# CREATE TABLE mart_tableau_root_cause_summary (
#     ingest_run_id TEXT,
#     root_cause_hint TEXT,
#     workload_pattern TEXT,
#     anomaly_count INTEGER,
#     critical_count INTEGER,
#     high_count INTEGER,
#     affected_devices INTEGER,
#     avg_anomaly_score REAL
# );

# -- One row per device per run for fast ad-hoc performance and trend analysis.
# CREATE TABLE mart_device_run_summary (
#     device TEXT,
#     ingest_run_id TEXT,
#     run_time TIMESTAMP WITH TIME ZONE,
#     avg_latency_ms REAL,
#     max_latency_ms REAL,
#     avg_util_pct REAL,
#     max_util_pct REAL,
#     avg_iops REAL,
#     avg_throughput_mb_s REAL,
#     avg_saturation_score REAL,
#     avg_aqu_sz REAL,
#     dominant_workload_pattern TEXT,
#     total_anomalies INTEGER,
#     critical_count INTEGER,
#     high_count INTEGER,
#     top_root_cause TEXT
# );

# -- Helper view with latest run IDs for curated/anomaly datasets.
# CREATE VIEW v_latest_run AS
# SELECT anomaly_run_id, curated_run_id FROM ...;

# RULES:
# 1. TABLE SELECTION (most important rule):
#      - Use mart_device_run_summary first for: historical performance, cross-device comparisons,
#          trend analysis, best/worst device, and balanced latency/utilization questions.
#      - Use curated_device_metrics for detailed timestamp-level performance drilldowns.
#      - Latest run IDs should come from v_latest_run:
#          curated latest: (SELECT curated_run_id FROM v_latest_run)
#          anomaly latest: (SELECT anomaly_run_id FROM v_latest_run)
#      - Use mart_tableau_root_cause_summary for: top root causes, workload vs root-cause distributions,
#          explanation summaries, aggregated root-cause/workload rankings, and cross-run root-cause summaries.
#    - Use anomaly_events for: anomaly counts, severity, anomaly score, root cause, detector type,
#          flagged events, critical/high/medium/low breakdowns, raw event-level root-cause inspection.
#      - For diagnostic questions containing why, explain, reason, cause, or root cause about performance,
#          prefer mart_device_run_summary if top_root_cause is sufficient; otherwise join curated_device_metrics and anomaly_events.
#    - You may JOIN both tables ON device AND ingest_run_id to correlate performance with anomalies.
#    - raw_device_metrics: only use if the question specifically asks for raw iostat fields
#      (r_s, w_s, rmb_s, wmb_s, r_await, w_await, svctm, iowait_pct, etc.).
# 2. Default to the latest ingest_run_id unless the question mentions "all runs", "across runs",
#    "over time", "historical", or a specific time range.
# 3. Cross-run queries: GROUP BY ingest_run_id, ORDER BY MIN(timestamp) ASC to show progression.
# 4. Text comparisons: always use ILIKE for device, severity, workload_pattern, root_cause_hint, metric_name.
# 5. Severity ordering: critical=4 > high=3 > medium=2 > low=1.
#    Use: CASE WHEN severity ILIKE 'critical' THEN 4 WHEN severity ILIKE 'high' THEN 3 WHEN severity ILIKE 'medium' THEN 2 ELSE 1 END
# 6. "Best performing" / "most stable" = ORDER BY relevant metric ASC (lower latency/util = better).
#    "Worst performing" = ORDER BY relevant metric DESC.
# 7. For percentiles: use PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY col).
# 8. For detecting all-detector coverage: HAVING COUNT(DISTINCT detector_type) >= 3.
# 9. Never generate INSERT, UPDATE, DELETE, DROP, TRUNCATE, ALTER, or CREATE.
# 10. CANNOT_ANSWER is a last resort — only use it if the question asks about data genuinely not
#     in any table (e.g. CPU temperature, network bandwidth, cost, memory). Never use it for trend,
#     cross-run, time-of-day, correlation, ranking, or percentile questions.

# EXAMPLES:

# Question: Which devices had critical anomalies in the last run?
# SQL: SELECT DISTINCT device FROM anomaly_events WHERE ingest_run_id = (SELECT ingest_run_id FROM anomaly_events WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) AND severity ILIKE 'critical';

# Question: What is the anomaly breakdown per device in the current run?
# SQL: SELECT device, severity, COUNT(*) AS count FROM anomaly_events WHERE ingest_run_id = (SELECT ingest_run_id FROM anomaly_events WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) GROUP BY device, severity ORDER BY device, count DESC;

# Question: Top root causes in the latest run?
# SQL: SELECT ingest_run_id, root_cause_hint, workload_pattern, anomaly_count, critical_count, affected_devices, ROUND(avg_anomaly_score::numeric, 3) AS avg_anomaly_score FROM mart_tableau_root_cause_summary WHERE ingest_run_id = (SELECT ingest_run_id FROM anomaly_events WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) ORDER BY critical_count DESC, anomaly_count DESC LIMIT 10;

# Question: Which workload pattern is most associated with queue buildup root causes?
# SQL: SELECT ingest_run_id, workload_pattern, root_cause_hint, anomaly_count, critical_count FROM mart_tableau_root_cause_summary WHERE ingest_run_id = (SELECT ingest_run_id FROM anomaly_events WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) AND root_cause_hint ILIKE '%queue%' ORDER BY anomaly_count DESC, critical_count DESC LIMIT 10;

# Question: Summarize root causes by workload pattern
# SQL: SELECT ingest_run_id, workload_pattern, root_cause_hint, anomaly_count, critical_count, affected_devices FROM mart_tableau_root_cause_summary WHERE ingest_run_id = (SELECT ingest_run_id FROM anomaly_events WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) ORDER BY critical_count DESC, anomaly_count DESC LIMIT 15;

# Question: Show root-cause trends across runs
# SQL: SELECT ingest_run_id, root_cause_hint, workload_pattern, anomaly_count, critical_count, affected_devices, ROUND(avg_anomaly_score::numeric, 3) AS avg_anomaly_score FROM mart_tableau_root_cause_summary ORDER BY ingest_run_id, critical_count DESC, anomaly_count DESC;

# Question: Is sda getting worse over time?
# SQL: SELECT ingest_run_id, MIN(timestamp) AS run_time, COUNT(*) AS anomaly_count, ROUND(AVG(anomaly_score)::numeric, 3) AS avg_score, SUM(CASE WHEN severity ILIKE 'critical' THEN 1 ELSE 0 END) AS critical_count FROM anomaly_events WHERE device ILIKE 'sda' GROUP BY ingest_run_id ORDER BY run_time ASC;

# Question: Did critical anomalies increase or decrease between the last two runs?
# SQL: WITH runs AS (SELECT ingest_run_id, MIN(timestamp) AS run_time, SUM(CASE WHEN severity ILIKE 'critical' THEN 1 ELSE 0 END) AS critical_count FROM anomaly_events GROUP BY ingest_run_id ORDER BY run_time DESC LIMIT 2) SELECT * FROM runs ORDER BY run_time ASC;

# Question: Which device performed best across all runs?
# SQL: SELECT device, COUNT(*) AS total_anomalies, ROUND(AVG(anomaly_score)::numeric, 3) AS avg_score FROM anomaly_events GROUP BY device ORDER BY total_anomalies ASC LIMIT 5;

# Question: Which device had the most critical anomalies overall?
# SQL: SELECT device, SUM(CASE WHEN severity ILIKE 'critical' THEN 1 ELSE 0 END) AS critical_count FROM anomaly_events GROUP BY device ORDER BY critical_count DESC LIMIT 1;

# Question: What is the 95th percentile anomaly score per device?
# SQL: SELECT device, ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY anomaly_score)::numeric, 3) AS p95_score FROM anomaly_events GROUP BY device ORDER BY p95_score DESC;

# Question: Which device has degraded the most in saturation score across runs?
# SQL: WITH run_stats AS (SELECT device, ingest_run_id, MIN(timestamp) AS run_time, AVG(saturation_score) AS avg_saturation FROM curated_device_metrics GROUP BY device, ingest_run_id), ranked AS (SELECT device, ingest_run_id, run_time, avg_saturation, ROW_NUMBER() OVER (PARTITION BY device ORDER BY run_time) AS run_num FROM run_stats) SELECT device, ROUND(REGR_SLOPE(avg_saturation, run_num)::numeric, 4) AS saturation_slope FROM ranked GROUP BY device ORDER BY saturation_slope DESC;

# Question: Which devices had high latency AND high queue depth at the same time in the last run?
# SQL: SELECT device, timestamp, avg_latency_ms, aqu_sz FROM curated_device_metrics WHERE ingest_run_id = (SELECT ingest_run_id FROM curated_device_metrics WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) AND avg_latency_ms > 10 AND aqu_sz > 2 ORDER BY avg_latency_ms DESC;

# Question: Which devices were flagged by all three detectors in the last run?
# SQL: SELECT device, COUNT(DISTINCT detector_type) AS detector_count FROM anomaly_events WHERE ingest_run_id = (SELECT ingest_run_id FROM anomaly_events WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) GROUP BY device HAVING COUNT(DISTINCT detector_type) >= 3;

# Question: Show anomalies where root cause was queue buildup but utilization was below 70%.
# SQL: SELECT device, timestamp, metric_name, severity, util_pct, root_cause_hint FROM anomaly_events WHERE ingest_run_id = (SELECT ingest_run_id FROM anomaly_events WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) AND root_cause_hint ILIKE '%queue%' AND util_pct < 70 ORDER BY anomaly_score DESC;

# Question: Do anomalies on sdb happen more during business hours or off hours?
# SQL: SELECT CASE WHEN hour_of_day BETWEEN 9 AND 17 THEN 'business_hours' ELSE 'off_hours' END AS period, COUNT(*) AS anomaly_count FROM anomaly_events WHERE device ILIKE 'sdb' GROUP BY period ORDER BY anomaly_count DESC;

# Question: Which hour of the day consistently produces the most saturation events?
# SQL: SELECT hour_of_day, COUNT(*) AS event_count FROM anomaly_events WHERE metric_name ILIKE '%saturation%' GROUP BY hour_of_day ORDER BY event_count DESC LIMIT 5;

# Question: Are write-heavy workloads more likely to produce critical anomalies than read-heavy ones?
# SQL: SELECT workload_pattern, COUNT(*) AS total, SUM(CASE WHEN severity ILIKE 'critical' THEN 1 ELSE 0 END) AS critical_count, ROUND(100.0 * SUM(CASE WHEN severity ILIKE 'critical' THEN 1 ELSE 0 END) / COUNT(*), 1) AS critical_pct FROM anomaly_events WHERE workload_pattern IN ('write_heavy', 'read_heavy') GROUP BY workload_pattern ORDER BY critical_pct DESC;

# Question: Which single timestamp had the worst combination of latency, queue depth, and utilization?
# SQL: SELECT device, timestamp, avg_latency_ms, aqu_sz, util_pct, ROUND((avg_latency_ms * 0.4 + aqu_sz * 30 + util_pct * 0.3)::numeric, 2) AS composite_stress FROM curated_device_metrics ORDER BY composite_stress DESC LIMIT 1;

# Question: Historical performance of sda
# SQL: SELECT device, ingest_run_id, MIN(timestamp) AS run_time, ROUND(AVG(avg_latency_ms)::numeric, 2) AS avg_latency_ms, ROUND(AVG(util_pct)::numeric, 2) AS avg_util_pct, ROUND(AVG(total_iops)::numeric, 0) AS avg_iops, ROUND(AVG(saturation_score)::numeric, 3) AS avg_saturation FROM curated_device_metrics WHERE device ILIKE 'sda' GROUP BY device, ingest_run_id ORDER BY run_time ASC;

# Question: Show me average latency per device across all runs
# SQL: SELECT device, ingest_run_id, MIN(timestamp) AS run_time, ROUND(AVG(avg_latency_ms)::numeric, 2) AS avg_latency_ms FROM curated_device_metrics GROUP BY device, ingest_run_id ORDER BY device, run_time ASC;

# Question: Which device has the highest average utilization in the current run?
# SQL: SELECT device, avg_util_pct FROM mart_device_run_summary WHERE ingest_run_id = (SELECT curated_run_id FROM v_latest_run) ORDER BY avg_util_pct DESC;

# Question: Show throughput and IOPS trends for nvme0n1 over all runs
# SQL: SELECT device, ingest_run_id, MIN(timestamp) AS run_time, ROUND(AVG(total_iops)::numeric, 0) AS avg_iops, ROUND(AVG(total_throughput_mb_s)::numeric, 2) AS avg_throughput_mb_s FROM curated_device_metrics WHERE device ILIKE 'nvme0n1' GROUP BY device, ingest_run_id ORDER BY run_time ASC;

# Question: Which devices are showing high saturation scores in the latest run?
# SQL: SELECT device, ROUND(AVG(saturation_score)::numeric, 3) AS avg_saturation, ROUND(MAX(saturation_score)::numeric, 3) AS max_saturation FROM curated_device_metrics WHERE ingest_run_id = (SELECT ingest_run_id FROM curated_device_metrics WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) GROUP BY device ORDER BY avg_saturation DESC;

# Question: What is the average read/write ratio per device?
# SQL: SELECT device, ROUND(AVG(read_ratio)::numeric, 3) AS avg_read_ratio, ROUND(AVG(write_ratio)::numeric, 3) AS avg_write_ratio FROM curated_device_metrics WHERE ingest_run_id = (SELECT ingest_run_id FROM curated_device_metrics WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) GROUP BY device ORDER BY avg_read_ratio DESC;

# Question: Compare latency and utilization between sda and sdb across all runs
# SQL: SELECT device, ingest_run_id, run_time, avg_latency_ms, avg_util_pct FROM mart_device_run_summary WHERE device ILIKE 'sda' OR device ILIKE 'sdb' ORDER BY device, run_time ASC;

# Question: Historical performance of sda with root cause across runs
# SQL: WITH perf AS (SELECT device, ingest_run_id, MIN(timestamp) AS run_time, ROUND(AVG(avg_latency_ms)::numeric, 2) AS avg_latency_ms, ROUND(AVG(util_pct)::numeric, 2) AS avg_util_pct, ROUND(AVG(total_iops)::numeric, 0) AS avg_iops, ROUND(AVG(total_throughput_mb_s)::numeric, 2) AS avg_throughput_mb_s, ROUND(AVG(saturation_score)::numeric, 3) AS avg_saturation FROM curated_device_metrics WHERE device ILIKE 'sda' GROUP BY device, ingest_run_id), cause_counts AS (SELECT device, ingest_run_id, root_cause_hint, workload_pattern, COUNT(*) AS root_cause_events, SUM(CASE WHEN severity ILIKE 'critical' THEN 1 ELSE 0 END) AS critical_events FROM anomaly_events WHERE device ILIKE 'sda' GROUP BY device, ingest_run_id, root_cause_hint, workload_pattern), ranked_causes AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY device, ingest_run_id ORDER BY root_cause_events DESC, critical_events DESC) AS cause_rank FROM cause_counts) SELECT perf.device, perf.ingest_run_id, perf.run_time, perf.avg_latency_ms, perf.avg_util_pct, perf.avg_iops, perf.avg_throughput_mb_s, perf.avg_saturation, ranked_causes.root_cause_hint AS top_root_cause, ranked_causes.workload_pattern AS root_cause_workload, ranked_causes.root_cause_events FROM perf LEFT JOIN ranked_causes ON perf.device = ranked_causes.device AND perf.ingest_run_id = ranked_causes.ingest_run_id AND ranked_causes.cause_rank = 1 ORDER BY perf.run_time ASC;

# Question: Why is sdb latency high in the latest run?
# SQL: WITH perf AS (SELECT device, ROUND(AVG(avg_latency_ms)::numeric, 2) AS avg_latency_ms, ROUND(AVG(util_pct)::numeric, 2) AS avg_util_pct, ROUND(AVG(total_iops)::numeric, 0) AS avg_iops, ROUND(AVG(total_throughput_mb_s)::numeric, 2) AS avg_throughput_mb_s, ROUND(AVG(saturation_score)::numeric, 3) AS avg_saturation FROM curated_device_metrics WHERE ingest_run_id = (SELECT ingest_run_id FROM curated_device_metrics WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) AND device ILIKE 'sdb' GROUP BY device), cause_counts AS (SELECT device, root_cause_hint, workload_pattern, COUNT(*) AS root_cause_events, SUM(CASE WHEN severity ILIKE 'critical' THEN 1 ELSE 0 END) AS critical_events FROM anomaly_events WHERE ingest_run_id = (SELECT ingest_run_id FROM anomaly_events WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) AND device ILIKE 'sdb' GROUP BY device, root_cause_hint, workload_pattern), ranked_causes AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY root_cause_events DESC, critical_events DESC) AS cause_rank FROM cause_counts) SELECT perf.device, perf.avg_latency_ms, perf.avg_util_pct, perf.avg_iops, perf.avg_throughput_mb_s, perf.avg_saturation, ranked_causes.root_cause_hint AS top_root_cause, ranked_causes.workload_pattern AS root_cause_workload, ranked_causes.root_cause_events FROM perf LEFT JOIN ranked_causes ON perf.device = ranked_causes.device AND ranked_causes.cause_rank = 1;

# Question: Compare sda and sdb in the latest run and explain why
# SQL: WITH perf AS (SELECT device, ROUND(AVG(avg_latency_ms)::numeric, 2) AS avg_latency_ms, ROUND(AVG(util_pct)::numeric, 2) AS avg_util_pct, ROUND(AVG(total_iops)::numeric, 0) AS avg_iops, ROUND(AVG(total_throughput_mb_s)::numeric, 2) AS avg_throughput_mb_s, ROUND(AVG(saturation_score)::numeric, 3) AS avg_saturation FROM curated_device_metrics WHERE ingest_run_id = (SELECT ingest_run_id FROM curated_device_metrics WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) AND (device ILIKE 'sda' OR device ILIKE 'sdb') GROUP BY device), cause_counts AS (SELECT device, root_cause_hint, workload_pattern, COUNT(*) AS root_cause_events, SUM(CASE WHEN severity ILIKE 'critical' THEN 1 ELSE 0 END) AS critical_events FROM anomaly_events WHERE ingest_run_id = (SELECT ingest_run_id FROM anomaly_events WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) AND (device ILIKE 'sda' OR device ILIKE 'sdb') GROUP BY device, root_cause_hint, workload_pattern), ranked_causes AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY root_cause_events DESC, critical_events DESC) AS cause_rank FROM cause_counts) SELECT perf.device, perf.avg_latency_ms, perf.avg_util_pct, perf.avg_iops, perf.avg_throughput_mb_s, perf.avg_saturation, ranked_causes.root_cause_hint AS top_root_cause, ranked_causes.workload_pattern AS root_cause_workload, ranked_causes.root_cause_events FROM perf LEFT JOIN ranked_causes ON perf.device = ranked_causes.device AND ranked_causes.cause_rank = 1 ORDER BY perf.avg_latency_ms DESC;

# Question: Compare sda and sdb across all runs and explain why
# SQL: WITH perf AS (SELECT device, ingest_run_id, MIN(timestamp) AS run_time, ROUND(AVG(avg_latency_ms)::numeric, 2) AS avg_latency_ms, ROUND(AVG(util_pct)::numeric, 2) AS avg_util_pct, ROUND(AVG(total_iops)::numeric, 0) AS avg_iops, ROUND(AVG(total_throughput_mb_s)::numeric, 2) AS avg_throughput_mb_s, ROUND(AVG(saturation_score)::numeric, 3) AS avg_saturation FROM curated_device_metrics WHERE device ILIKE 'sda' OR device ILIKE 'sdb' GROUP BY device, ingest_run_id), cause_counts AS (SELECT device, ingest_run_id, root_cause_hint, workload_pattern, COUNT(*) AS root_cause_events, SUM(CASE WHEN severity ILIKE 'critical' THEN 1 ELSE 0 END) AS critical_events FROM anomaly_events WHERE device ILIKE 'sda' OR device ILIKE 'sdb' GROUP BY device, ingest_run_id, root_cause_hint, workload_pattern), ranked_causes AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY device, ingest_run_id ORDER BY root_cause_events DESC, critical_events DESC) AS cause_rank FROM cause_counts) SELECT perf.device, perf.ingest_run_id, perf.run_time, perf.avg_latency_ms, perf.avg_util_pct, perf.avg_iops, perf.avg_throughput_mb_s, perf.avg_saturation, ranked_causes.root_cause_hint AS top_root_cause, ranked_causes.workload_pattern AS root_cause_workload, ranked_causes.root_cause_events FROM perf LEFT JOIN ranked_causes ON perf.device = ranked_causes.device AND perf.ingest_run_id = ranked_causes.ingest_run_id AND ranked_causes.cause_rank = 1 ORDER BY perf.device, perf.run_time ASC;
# """.strip()


# # ---------------------------------------------------------------------------
# # Retry prompts for attempts 2 and 3
# # ---------------------------------------------------------------------------

# NUDGE_SUFFIX = """

# IMPORTANT: You must return a valid PostgreSQL SELECT query.
# Do not return CANNOT_ANSWER — the schema above contains everything needed to answer this.
# An imperfect query is better than no answer — it will be validated before execution.
# """.strip()

# CHAIN_OF_THOUGHT_TEMPLATE = """
# Think step by step before writing the SQL:

# 1. Which table(s) are relevant?
#     - mart_device_run_summary: per-device-per-run performance + anomaly/root-cause rollups (primary)
#     - curated_device_metrics: timestamp-level performance details
#     - mart_tableau_root_cause_summary: aggregated root-cause/workload summary, explanation storytelling, rankings
#    - anomaly_events: anomaly counts, severity, scores, root cause, detector type
#    - raw_device_metrics: only for raw iostat fields (r_s, w_s, r_await, etc.)
#     - For why/explain performance questions, prefer mart_device_run_summary top_root_cause first.
# 2. Does this need the latest run only, or all runs?
#    - Latest run only: WHERE ingest_run_id = (SELECT ingest_run_id FROM <chosen_table> WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1)
#    - All runs / trend / historical / comparison: GROUP BY ingest_run_id ORDER BY MIN(timestamp) ASC
# 3. What should I GROUP BY? (include all non-aggregate SELECT columns)
# 4. What should I ORDER BY?
#    - Performance: lower latency/util/saturation = better
#    - Anomalies: fewer = better health (ORDER BY COUNT(*) ASC for "best")
# 5. Do I need a CTE (WITH clause) for multi-step logic?

# Now write ONLY the final SQL query. No explanation, no markdown, no backticks.

# Question: {question}
# """.strip()


# # ---------------------------------------------------------------------------
# # Safety + cleaning
# # ---------------------------------------------------------------------------

# def _ensure_safe_select(sql: str) -> str:
#     """Reject any SQL that is not a plain SELECT or CTE."""
#     stripped = sql.strip().lower()
#     if any(kw in stripped for kw in FORBIDDEN_KEYWORDS):
#         return "CANNOT_ANSWER"
#     if not (stripped.startswith("select") or stripped.startswith("with")):
#         return "CANNOT_ANSWER"
#     return sql


# def _clean_llm_output(text: str) -> str:
#     """Strip markdown fences and 'SQL:' prefixes from LLM output."""
#     out = text.strip()
#     out = re.sub(r"```sql\s*", "", out, flags=re.IGNORECASE)
#     out = re.sub(r"```", "", out)
#     if out.lower().startswith("sql:"):
#         out = out[4:]
#     return out.strip()


# # ---------------------------------------------------------------------------
# # Ollama transport
# # ---------------------------------------------------------------------------

# def _call_ollama(messages: list[dict], model: str, host: str, timeout: int = 120) -> str:
#     """POST a chat request to Ollama. Raises RuntimeError if unreachable."""
#     url = host.rstrip("/") + "/api/chat"
#     payload = {
#         "model": model,
#         "messages": messages,
#         "stream": False,
#         "options": {"temperature": 0},
#     }
#     data = json.dumps(payload).encode("utf-8")
#     req = request.Request(
#         url, data=data,
#         headers={"Content-Type": "application/json"},
#         method="POST",
#     )
#     try:
#         with request.urlopen(req, timeout=timeout) as resp:
#             body = json.loads(resp.read().decode("utf-8"))
#             return body.get("message", {}).get("content", "").strip()
#     except (error.URLError, TimeoutError, OSError) as exc:
#         raise RuntimeError(f"Ollama unreachable: {exc}") from exc


# def _attempt(system: str, user: str, model: str, host: str) -> str:
#     """Single generation attempt. Returns cleaned SQL or 'CANNOT_ANSWER'."""
#     messages = [
#         {"role": "system", "content": system},
#         {"role": "user", "content": user},
#     ]
#     raw = _call_ollama(messages, model=model, host=host)
#     sql = _clean_llm_output(raw)
#     if sql.upper() == "CANNOT_ANSWER":
#         return "CANNOT_ANSWER"
#     return _ensure_safe_select(sql)


# # ---------------------------------------------------------------------------
# # LLM SQL generation — three attempts
# # ---------------------------------------------------------------------------

# def generate_sql_llm(question: str, model: str, host: str) -> str:
#     """
#     Three-attempt LLM SQL generation:

#     Attempt 1 — standard prompt + examples: handles the majority of questions.
#     Attempt 2 — nudge: explicitly tells the model not to give up.
#     Attempt 3 — chain-of-thought: asks the model to reason step by step before
#                 writing SQL, handles complex multi-condition and cross-run questions
#                 that fail attempts 1 and 2.

#     Returns SQL string or 'CANNOT_ANSWER' if all three attempts fail.
#     """
#     # Attempt 1
#     sql = _attempt(SQL_SYSTEM_PROMPT, f"Question: {question}", model, host)
#     if sql != "CANNOT_ANSWER":
#         return sql

#     # Attempt 2 — nudge
#     sql = _attempt(
#         SQL_SYSTEM_PROMPT + "\n\n" + NUDGE_SUFFIX,
#         f"Question: {question}",
#         model, host,
#     )
#     if sql != "CANNOT_ANSWER":
#         return sql

#     # Attempt 3 — chain-of-thought
#     cot_user = CHAIN_OF_THOUGHT_TEMPLATE.format(question=question)
#     return _attempt(SQL_SYSTEM_PROMPT, cot_user, model, host)


# # ---------------------------------------------------------------------------
# # Minimal rule-based fallback (Ollama unavailable only)
# # ---------------------------------------------------------------------------

# def _latest(table: str) -> str:
#     if table == "curated_device_metrics":
#         return "(SELECT curated_run_id FROM v_latest_run)"
#     if table == "anomaly_events":
#         return "(SELECT anomaly_run_id FROM v_latest_run)"
#     return (
#         f"(SELECT ingest_run_id FROM {table} "
#         "WHERE ingest_run_id IS NOT NULL "
#         "ORDER BY timestamp DESC LIMIT 1)"
#     )


# def generate_sql_rule_based(question: str) -> str:
#     """
#     Minimal rule-based fallback used only when Ollama is unreachable.
#     Covers a small set of high-confidence patterns; returns CANNOT_ANSWER
#     for everything else rather than producing a wrong answer.
#     """
#     q = question.lower().replace("-", " ").strip()

#     if "critical" in q and ("last run" in q or "latest run" in q) and "device" in q:
#         return _ensure_safe_select(
#             f"SELECT DISTINCT device FROM anomaly_events "
#             f"WHERE ingest_run_id = {_latest('anomaly_events')} "
#             f"AND severity ILIKE 'critical';"
#         )

#     if (
#         "root cause" in q
#         and any(term in q for term in ("top", "summary", "summarize", "most", "distribution", "workload", "trend", "trends"))
#         and "device" not in q
#         and "timestamp" not in q
#     ):
#         is_cross_run = any(term in q for term in ("historical", "over time", "across runs", "per run", "trend", "all runs"))
#         where_clauses = []
#         if "queue" in q:
#             where_clauses.append("root_cause_hint ILIKE '%queue%'")
#         if "saturation" in q:
#             where_clauses.append("root_cause_hint ILIKE '%saturation%'")
#         if "write" in q:
#             where_clauses.append("workload_pattern ILIKE '%write%'")
#         if "read" in q:
#             where_clauses.append("workload_pattern ILIKE '%read%'")
#         if not is_cross_run:
#             where_clauses.append(f"ingest_run_id = {_latest('anomaly_events')}")
#         where_sql = f"WHERE {' AND '.join(where_clauses)} " if where_clauses else ""
#         if is_cross_run:
#             return _ensure_safe_select(
#                 "WITH ranked_causes AS ("
#                 "SELECT ingest_run_id, root_cause_hint, workload_pattern, anomaly_count, critical_count, affected_devices, "
#                 "ROUND(avg_anomaly_score::numeric, 3) AS avg_anomaly_score, "
#                 "ROW_NUMBER() OVER (PARTITION BY ingest_run_id ORDER BY critical_count DESC, anomaly_count DESC) AS rank_in_run "
#                 "FROM mart_tableau_root_cause_summary "
#                 f"{where_sql}"
#                 ") "
#                 "SELECT ingest_run_id, root_cause_hint, workload_pattern, anomaly_count, critical_count, affected_devices, avg_anomaly_score "
#                 "FROM ranked_causes WHERE rank_in_run <= 3 ORDER BY ingest_run_id, rank_in_run;"
#             )
#         return _ensure_safe_select(
#             "SELECT ingest_run_id, root_cause_hint, workload_pattern, anomaly_count, critical_count, affected_devices, "
#             "ROUND(avg_anomaly_score::numeric, 3) AS avg_anomaly_score "
#             "FROM mart_tableau_root_cause_summary "
#             f"{where_sql}"
#             "ORDER BY critical_count DESC, anomaly_count DESC LIMIT 15;"
#         )

#     # Performance / curated metrics questions
#     curated_perf_keywords = ("latency", "utilization", "util", "iops", "throughput", "saturation", "workload pattern", "read ratio", "write ratio", "queue depth", "queue efficiency")
#     is_perf_question = any(kw in q for kw in curated_perf_keywords)
#     m_device = re.search(r"\b(sda|sdb|nvme\S+|dm-\d+)\b", q)
#     devices = list(dict.fromkeys(re.findall(r"\b(sda|sdb|nvme\S+|dm-\d+)\b", q)))
#     wants_root_cause_context = any(term in q for term in ("root cause", "why", "reason", "cause"))
#     is_diagnostic_compare = "compare" in q and len(devices) >= 2 and wants_root_cause_context

#     if is_perf_question or "historical" in q or "performance of" in q or is_diagnostic_compare:
#         # Determine primary sort metric based on what the question asks about
#         if any(kw in q for kw in ("utilization", "util")):
#             order_col = "avg_util_pct"
#         elif any(kw in q for kw in ("iops",)):
#             order_col = "avg_iops"
#         elif any(kw in q for kw in ("throughput",)):
#             order_col = "avg_throughput_mb_s"
#         elif any(kw in q for kw in ("saturation",)):
#             order_col = "avg_saturation_score"
#         else:
#             order_col = "avg_latency_ms"

#         device_filter = f"WHERE device ILIKE '{m_device.group(1)}'" if m_device else ""
#         is_all_runs = any(p in q for p in ("all runs", "across runs", "historical", "over time", "trend"))
#         device_predicate = " OR ".join(f"device ILIKE '{device}'" for device in devices)
#         use_multi_device_root_cause = wants_root_cause_context and len(devices) >= 2

#         select_perf_cols = (
#             "device, ingest_run_id, run_time, avg_latency_ms, avg_util_pct, avg_iops, "
#             "avg_throughput_mb_s, avg_saturation_score AS avg_saturation"
#         )
#         select_diag_cols = ", total_anomalies, critical_count, high_count, top_root_cause, dominant_workload_pattern"
#         select_cols = select_perf_cols + (select_diag_cols if wants_root_cause_context else "")

#         if is_all_runs:
#             if use_multi_device_root_cause:
#                 where = f"WHERE {device_predicate}"
#             elif m_device:
#                 where = device_filter
#             else:
#                 where = ""
#             return _ensure_safe_select(
#                 f"SELECT {select_cols} "
#                 f"FROM mart_device_run_summary "
#                 f"{where + ' ' if where else ''}"
#                 f"ORDER BY device, run_time ASC;"
#             )
#         else:
#             latest = _latest("curated_device_metrics")
#             where_parts = [f"ingest_run_id = {latest}"]
#             if use_multi_device_root_cause:
#                 where_parts.append(f"({device_predicate})")
#             elif m_device:
#                 where_parts.append(f"device ILIKE '{m_device.group(1)}'")
#             return _ensure_safe_select(
#                 f"SELECT device, avg_latency_ms, avg_util_pct, avg_iops, avg_throughput_mb_s, "
#                 f"avg_saturation_score AS avg_saturation"
#                 f"{select_diag_cols if wants_root_cause_context else ''} "
#                 f"FROM mart_device_run_summary "
#                 f"WHERE {' AND '.join(where_parts)} "
#                 f"ORDER BY {order_col} DESC;"
#             )

#     if ("average latency" in q or "avg latency" in q) and "per device" in q:
#         return _ensure_safe_select(
#             f"SELECT device, ROUND(AVG(avg_latency_ms)::numeric, 2) AS avg_latency_ms "
#             f"FROM curated_device_metrics "
#             f"WHERE ingest_run_id = {_latest('curated_device_metrics')} "
#             f"GROUP BY device ORDER BY avg_latency_ms DESC;"
#         )

#     if "root cause" in q and "month" in q:
#         return _ensure_safe_select(
#             "SELECT root_cause_hint, COUNT(*) AS occurrences FROM anomaly_events "
#             "WHERE timestamp >= date_trunc('month', NOW()) "
#             "GROUP BY root_cause_hint ORDER BY occurrences DESC LIMIT 10;"
#         )

#     if ("anomalies" in q or "anomaly count" in q) and "per run" in q:
#         return _ensure_safe_select(
#             "SELECT ingest_run_id, MIN(timestamp) AS run_time, COUNT(*) AS anomaly_count "
#             "FROM anomaly_events GROUP BY ingest_run_id ORDER BY run_time ASC;"
#         )

#     if (
#         ("compare" in q or "performance" in q or "performed" in q)
#         and (
#             "all runs" in q
#             or "all the runs" in q
#             or "compare all runs" in q
#             or "across all" in q
#             or "across runs" in q
#         )
#         and "device" in q
#     ):
#         order_dir = "ASC" if ("best" in q or "fewest" in q or "most stable" in q) else "DESC"
#         return _ensure_safe_select(
#             "SELECT device, COUNT(*) AS total_anomalies, "
#             "ROUND(AVG(anomaly_score)::numeric, 3) AS avg_score, "
#             "SUM(CASE WHEN severity ILIKE 'critical' THEN 1 ELSE 0 END) AS critical_count "
#             "FROM anomaly_events "
#             f"GROUP BY device ORDER BY total_anomalies {order_dir} LIMIT 15;"
#         )

#     m = re.search(r"\b(sda|sdb|nvme\S+|dm-\d+)\b", q)
#     if m and ("getting worse" in q or "trend" in q or "over time" in q):
#         device = m.group(1)
#         return _ensure_safe_select(
#             f"SELECT ingest_run_id, MIN(timestamp) AS run_time, COUNT(*) AS anomaly_count, "
#             f"ROUND(AVG(anomaly_score)::numeric, 3) AS avg_score "
#             f"FROM anomaly_events WHERE device ILIKE '{device}' "
#             f"GROUP BY ingest_run_id ORDER BY run_time ASC;"
#         )

#     if "worst" in q or "most severe" in q:
#         return _ensure_safe_select(
#             f"SELECT device, timestamp, metric_name, severity, anomaly_score, root_cause_hint "
#             f"FROM anomaly_events "
#             f"WHERE ingest_run_id = {_latest('anomaly_events')} "
#             f"ORDER BY CASE WHEN severity ILIKE 'critical' THEN 4 WHEN severity ILIKE 'high' THEN 3 "
#             f"WHEN severity ILIKE 'medium' THEN 2 ELSE 1 END DESC, anomaly_score DESC LIMIT 25;"
#         )

#     return "CANNOT_ANSWER"


# # ---------------------------------------------------------------------------
# # Public entry point
# # ---------------------------------------------------------------------------

# def generate_sql(
#     question: str,
#     use_llm: bool = False,
#     llm_model: str = "llama3.1:8b",
#     llm_host: str = "http://localhost:11434",
#     allow_best_effort: bool = True,
# ) -> str:
#     """
#     Generate a safe SELECT query for the given natural language question.

#     With use_llm=True and Ollama reachable: three-attempt LLM generation.
#     With use_llm=True and Ollama unreachable: warns and falls back to rule-based.
#     With use_llm=False: rule-based only (limited pattern coverage, honest CANNOT_ANSWER).
#     allow_best_effort is accepted for backwards compatibility with older CLI calls.
#     """
#     if use_llm:
#         try:
#             return generate_sql_llm(question, model=llm_model, host=llm_host)
#         except RuntimeError as exc:
#             print(f"[sql_assistant] Ollama unavailable ({exc}). Falling back to rule-based.")

#     return generate_sql_rule_based(question)


# _AGGREGATE_PATTERN = re.compile(
#     r"\b(COUNT|SUM|AVG|MIN|MAX|ROUND|STDDEV|VARIANCE|STRING_AGG|ARRAY_AGG)\s*\(",
#     re.IGNORECASE,
# )


# def _fix_group_by(sql: str) -> str:
#     """
#     Auto-repair a common LLM error: column appears in SELECT but is missing from GROUP BY.
#     Only adds simple bare column names (no aliases, no expressions) to the GROUP BY list.
#     """
#     if "group by" not in sql.lower():
#         return sql

#     # Extract SELECT list and GROUP BY list separately
#     select_m = re.search(r"SELECT\s+(.*?)\s+FROM\s", sql, re.IGNORECASE | re.DOTALL)
#     group_m = re.search(
#         r"GROUP\s+BY\s+([\w\s,]+?)(?:\s+ORDER\s+BY|\s+HAVING|\s+LIMIT|;|$)",
#         sql,
#         re.IGNORECASE,
#     )
#     if not select_m or not group_m:
#         return sql

#     select_clause = select_m.group(1)
#     group_by_str = group_m.group(1).strip()
#     group_by_cols = {c.strip().lower() for c in group_by_str.split(",")}

#     missing = []
#     for part in select_clause.split(","):
#         part = part.strip()
#         if _AGGREGATE_PATTERN.search(part):
#             continue  # skip aggregate expressions
#         if re.match(r"CASE\b", part, re.IGNORECASE):
#             continue  # skip CASE expressions
#         # Take the column name before any AS alias
#         col = re.split(r"\s+AS\s+", part, flags=re.IGNORECASE)[0].strip()
#         # Only plain identifiers (no dots, no parens)
#         if re.fullmatch(r"[\w]+", col) and col.lower() not in group_by_cols:
#             missing.append(col)

#     if not missing:
#         return sql

#     new_group_by = group_by_str + ", " + ", ".join(missing)
#     return re.sub(
#         r"(GROUP\s+BY\s+)([\w\s,]+?)(\s+ORDER\s+BY|\s+HAVING|\s+LIMIT|;|$)",
#         lambda m: m.group(1) + new_group_by + m.group(3),
#         sql,
#         flags=re.IGNORECASE,
#     )


# def run_query(sql: str) -> pd.DataFrame:
#     """Execute a validated SELECT query and return results as a DataFrame."""
#     sql = _fix_group_by(sql)
#     engine = get_engine()
#     with engine.connect() as conn:
#         return pd.read_sql(text(sql), conn)


# # ---------------------------------------------------------------------------
# # Result summarization
# # ---------------------------------------------------------------------------

# SUMMARIZE_SYSTEM_PROMPT = """
# You are a data analyst assistant for a storage telemetry platform.
# You will be given a question and the raw CSV results of a SQL query.
# Summarize the findings in 2-3 plain English sentences.
# Be specific: include device names, metric values, and numbers from the results.
# If results show a trend across runs, describe the direction clearly: improving, degrading, or stable.
# Do not mention SQL, tables, or column names.
# """.strip()


# def summarize_results_llm(
#     question: str,
#     results_df: pd.DataFrame,
#     model: str,
#     host: str,
# ) -> str:
#     csv_preview = results_df.head(200).to_csv(index=False)
#     messages = [
#         {"role": "system", "content": SUMMARIZE_SYSTEM_PROMPT},
#         {"role": "user", "content": f"Question: {question}\n\nResults:\n{csv_preview}"},
#     ]
#     return _call_ollama(messages, model=model, host=host)


# def _fallback_summary(results_df: pd.DataFrame) -> str:
#     """Minimal deterministic summary used when LLM is unavailable."""
#     row_count = len(results_df)
#     cols = list(results_df.columns)
#     preview = results_df.head(5).to_dict(orient="records")
#     return (
#         f"Query returned {row_count} row(s) with columns: {', '.join(cols)}.\n"
#         f"Sample: {preview}"
#     )


# def _deterministic_summary(question: str, results_df: pd.DataFrame):
#     """Question-aware summary for common result shapes to avoid generic repeats."""
#     q = question.lower()

#     # Device ranking output (best/worst across device list)
#     if {"device", "total_anomalies"}.issubset(results_df.columns):
#         ranked = results_df.sort_values("total_anomalies", ascending=True)
#         best = ranked.iloc[0]
#         worst = ranked.iloc[-1]
#         return (
#             f"{best['device']} performed best with {int(best['total_anomalies']):,} total anomalies. "
#             f"{worst['device']} performed worst with {int(worst['total_anomalies']):,} anomalies."
#         )

#     # Root-cause/workload aggregated summary output
#     root_cols = {"root_cause_hint", "workload_pattern", "anomaly_count"}
#     if root_cols.issubset(results_df.columns):
#         if "ingest_run_id" in results_df.columns and results_df["ingest_run_id"].nunique() > 1:
#             per_run = (
#                 results_df.sort_values([c for c in ["ingest_run_id", "critical_count", "anomaly_count"] if c in results_df.columns], ascending=[True, False, False])
#                 .groupby("ingest_run_id", as_index=False)
#                 .first()
#             )
#             first_run = per_run.iloc[0]
#             last_run = per_run.iloc[-1]
#             return (
#                 f"Across {len(per_run)} runs, the top root cause in the earliest run was {first_run['root_cause_hint']} under {first_run['workload_pattern']} workload "
#                 f"with {int(first_run['anomaly_count']):,} anomalies. In the latest run, the top root cause was {last_run['root_cause_hint']} "
#                 f"under {last_run['workload_pattern']} workload with {int(last_run['anomaly_count']):,} anomalies."
#             )
#         sort_cols = [c for c in ["critical_count", "anomaly_count"] if c in results_df.columns]
#         ranked = results_df.sort_values(sort_cols, ascending=[False] * len(sort_cols))
#         top = ranked.iloc[0]
#         run_prefix = ""
#         if "ingest_run_id" in ranked.columns and ranked["ingest_run_id"].nunique() == 1:
#             run_prefix = f"Run {top['ingest_run_id']}: "
#         elif "ingest_run_id" in ranked.columns and ranked["ingest_run_id"].nunique() > 1:
#             run_count = ranked["ingest_run_id"].nunique()
#             run_prefix = f"Across {run_count} runs, "
#         parts = [
#             f"{run_prefix}top root cause: {top['root_cause_hint']} under {top['workload_pattern']} workload with {int(top['anomaly_count']):,} anomalies."
#         ]
#         if "critical_count" in ranked.columns:
#             parts.append(f"Critical anomalies for this cause: {int(top['critical_count']):,}.")
#         if "affected_devices" in ranked.columns:
#             parts.append(f"Affected devices: {int(top['affected_devices']):,}.")
#         if len(ranked) > 1:
#             second = ranked.iloc[1]
#             parts.append(
#                 f"Next most common: {second['root_cause_hint']} under {second['workload_pattern']} workload with {int(second['anomaly_count']):,} anomalies."
#             )
#         return " ".join(parts)

#     # Curated performance: multi-device snapshot in latest run
#     perf_cols = {"device", "avg_latency_ms", "avg_util_pct"}
#     if perf_cols.issubset(results_df.columns) and "ingest_run_id" not in results_df.columns:
#         df = results_df.copy()
#         if len(df) == 1:
#             row = df.iloc[0]
#             parts = [f"{row['device']}: avg latency {row['avg_latency_ms']:.2f} ms and utilization {row['avg_util_pct']:.1f}%. "]
#             if "avg_iops" in df.columns:
#                 parts.append(f"IOPS: {row['avg_iops']:.0f}.")
#             if "avg_throughput_mb_s" in df.columns:
#                 parts.append(f"Throughput: {row['avg_throughput_mb_s']:.2f} MB/s.")
#             if "avg_saturation" in df.columns:
#                 parts.append(f"Saturation score: {row['avg_saturation']:.3f}.")
#             if "top_root_cause" in df.columns and pd.notna(row.get("top_root_cause")):
#                 root_text = f"Likely root cause: {row['top_root_cause']}"
#                 if "root_cause_workload" in df.columns and pd.notna(row.get("root_cause_workload")):
#                     root_text += f" under {row['root_cause_workload']} workload"
#                 if "root_cause_events" in df.columns and pd.notna(row.get("root_cause_events")):
#                     root_text += f" ({int(row['root_cause_events'])} anomaly events)"
#                 parts.append(root_text + ".")
#             return " ".join(parts)
#         parts = []
#         # Lead with the metric the user specifically asked about
#         if any(kw in q for kw in ("utilization", "util")):
#             worst_util = df.loc[df["avg_util_pct"].idxmax()]
#             best_util = df.loc[df["avg_util_pct"].idxmin()]
#             parts.append(
#                 f"{worst_util['device']} has the highest utilization at {worst_util['avg_util_pct']:.1f}%, "
#                 f"{best_util['device']} the lowest at {best_util['avg_util_pct']:.1f}%."
#             )
#         elif any(kw in q for kw in ("iops",)):
#             worst_iops = df.loc[df["avg_iops"].idxmax()]
#             parts.append(f"{worst_iops['device']} has the highest IOPS at {worst_iops['avg_iops']:.0f}.")
#         elif any(kw in q for kw in ("throughput",)) and "avg_throughput_mb_s" in df.columns:
#             worst_tp = df.loc[df["avg_throughput_mb_s"].idxmax()]
#             parts.append(f"{worst_tp['device']} has the highest throughput at {worst_tp['avg_throughput_mb_s']:.2f} MB/s.")
#         elif any(kw in q for kw in ("saturation",)) and "avg_saturation" in df.columns:
#             worst_sat = df.loc[df["avg_saturation"].idxmax()]
#             parts.append(f"{worst_sat['device']} has the highest saturation score at {worst_sat['avg_saturation']:.3f}.")
#         else:
#             worst_lat = df.loc[df["avg_latency_ms"].idxmax()]
#             best_lat = df.loc[df["avg_latency_ms"].idxmin()]
#             parts.append(
#                 f"{worst_lat['device']} has the highest average latency ({worst_lat['avg_latency_ms']:.2f} ms) "
#                 f"and {best_lat['device']} the lowest ({best_lat['avg_latency_ms']:.2f} ms)."
#             )
#         # Always follow with remaining key metrics
#         if any(kw in q for kw in ("utilization", "util")) or "avg_latency_ms" in df.columns:
#             worst_lat = df.loc[df["avg_latency_ms"].idxmax()]
#             if not any(kw in q for kw in ("utilization", "util")):
#                 pass  # already added latency as lead
#             else:
#                 parts.append(f"Highest latency: {worst_lat['device']} at {worst_lat['avg_latency_ms']:.2f} ms.")
#         if "avg_util_pct" in df.columns and not any(kw in q for kw in ("utilization", "util")):
#             worst_util = df.loc[df["avg_util_pct"].idxmax()]
#             parts.append(f"{worst_util['device']} has the highest utilization at {worst_util['avg_util_pct']:.1f}%.")
#         if "avg_iops" in df.columns and not any(kw in q for kw in ("iops",)):
#             worst_iops = df.loc[df["avg_iops"].idxmax()]
#             parts.append(f"Highest IOPS: {worst_iops['device']} at {worst_iops['avg_iops']:.0f}.")
#         if "avg_throughput_mb_s" in df.columns and not any(kw in q for kw in ("throughput",)):
#             worst_tp = df.loc[df["avg_throughput_mb_s"].idxmax()]
#             parts.append(f"Highest throughput: {worst_tp['device']} at {worst_tp['avg_throughput_mb_s']:.2f} MB/s.")
#         if "avg_saturation" in df.columns and not any(kw in q for kw in ("saturation",)):
#             worst_sat = df.loc[df["avg_saturation"].idxmax()]
#             parts.append(f"Highest saturation score: {worst_sat['device']} at {worst_sat['avg_saturation']:.3f}.")
#         if "top_root_cause" in df.columns and df["top_root_cause"].notna().any() and len(df) > 1:
#             if any(term in q for term in ("why", "root cause", "reason", "cause")):
#                 top_row = df.iloc[0]
#                 focus = f"Likely reason for top device {top_row['device']}: {top_row['top_root_cause']}"
#                 if "root_cause_workload" in df.columns and pd.notna(top_row.get("root_cause_workload")):
#                     focus += f" under {top_row['root_cause_workload']} workload"
#                 if "root_cause_events" in df.columns and pd.notna(top_row.get("root_cause_events")):
#                     focus += f" ({int(top_row['root_cause_events'])} anomaly events)"
#                 parts.append(focus + ".")
#             cause_parts = []
#             for _, root_row in df[df["top_root_cause"].notna()].iterrows():
#                 cause_text = f"{root_row['device']}: {root_row['top_root_cause']}"
#                 if "root_cause_workload" in df.columns and pd.notna(root_row.get("root_cause_workload")):
#                     cause_text += f" under {root_row['root_cause_workload']} workload"
#                 cause_parts.append(cause_text)
#             parts.append("Likely root causes by device: " + "; ".join(cause_parts) + ".")
#         if "top_root_cause" in df.columns and df["top_root_cause"].notna().any() and len(df) == 1:
#             root_row = df[df["top_root_cause"].notna()].iloc[0]
#             root_text = f"Likely root cause: {root_row['top_root_cause']}"
#             if "root_cause_workload" in df.columns and pd.notna(root_row.get("root_cause_workload")):
#                 root_text += f" under {root_row['root_cause_workload']} workload"
#             if "root_cause_events" in df.columns and pd.notna(root_row.get("root_cause_events")):
#                 root_text += f" ({int(root_row['root_cause_events'])} anomaly events)"
#             parts.append(root_text + ".")
#         return " ".join(parts)

#     # Curated performance: per-run trend for a single device
#     perf_run_cols = {"device", "ingest_run_id", "run_time", "avg_latency_ms"}
#     if perf_run_cols.issubset(results_df.columns):
#         df = results_df.sort_values("run_time")
#         if df["device"].nunique() > 1:
#             latest_per_device = df.groupby("device", as_index=False).tail(1).sort_values("avg_latency_ms", ascending=False)
#             worst = latest_per_device.iloc[0]
#             best = latest_per_device.iloc[-1]
#             parts = [
#                 f"In the latest available run, {worst['device']} shows higher latency ({worst['avg_latency_ms']:.2f} ms) than {best['device']} ({best['avg_latency_ms']:.2f} ms)."
#             ]
#             if "avg_util_pct" in latest_per_device.columns:
#                 parts.append(
#                     f"Utilization is {worst['avg_util_pct']:.1f}% for {worst['device']} versus {best['avg_util_pct']:.1f}% for {best['device']}."
#                 )
#             if "top_root_cause" in latest_per_device.columns and latest_per_device["top_root_cause"].notna().any():
#                 cause_parts = []
#                 for _, root_row in latest_per_device[latest_per_device["top_root_cause"].notna()].iterrows():
#                     cause_text = f"{root_row['device']}: {root_row['top_root_cause']}"
#                     if "root_cause_workload" in latest_per_device.columns and pd.notna(root_row.get("root_cause_workload")):
#                         cause_text += f" under {root_row['root_cause_workload']} workload"
#                     cause_parts.append(cause_text)
#                 parts.append("Likely root causes in the latest run: " + "; ".join(cause_parts) + ".")
#             return " ".join(parts)
#         first = df.iloc[0]
#         last = df.iloc[-1]
#         device = first["device"]
#         if len(df) == 1:
#             parts = [f"{device} — only 1 run available."]
#             parts.append(f"Avg latency: {first['avg_latency_ms']:.2f} ms.")
#             if "avg_util_pct" in df.columns:
#                 parts.append(f"Utilization: {first['avg_util_pct']:.1f}%.")
#             if "avg_iops" in df.columns:
#                 parts.append(f"IOPS: {first['avg_iops']:.0f}.")
#             if "avg_throughput_mb_s" in df.columns:
#                 parts.append(f"Throughput: {first['avg_throughput_mb_s']:.2f} MB/s.")
#             if "avg_saturation" in df.columns:
#                 parts.append(f"Saturation score: {first['avg_saturation']:.3f}.")
#         else:
#             lat_dir = "increased" if last["avg_latency_ms"] > first["avg_latency_ms"] else "decreased" if last["avg_latency_ms"] < first["avg_latency_ms"] else "unchanged"
#             parts = [f"{device}: avg latency {lat_dir} from {first['avg_latency_ms']:.2f} ms to {last['avg_latency_ms']:.2f} ms across {len(df)} runs."]
#             if "avg_util_pct" in df.columns:
#                 util_dir = "increased" if last["avg_util_pct"] > first["avg_util_pct"] else "decreased"
#                 parts.append(f"Utilization {util_dir} from {first['avg_util_pct']:.1f}% to {last['avg_util_pct']:.1f}%.")
#             if "avg_iops" in df.columns:
#                 parts.append(f"Average IOPS: {last['avg_iops']:.0f} in the latest run.")
#             if "avg_throughput_mb_s" in df.columns:
#                 tp_dir = "up" if last["avg_throughput_mb_s"] > first["avg_throughput_mb_s"] else "down"
#                 parts.append(f"Throughput {tp_dir} from {first['avg_throughput_mb_s']:.2f} to {last['avg_throughput_mb_s']:.2f} MB/s.")
#             if "avg_saturation" in df.columns:
#                 sat_dir = "up" if last["avg_saturation"] > first["avg_saturation"] else "down"
#                 parts.append(f"Saturation score {sat_dir} from {first['avg_saturation']:.3f} to {last['avg_saturation']:.3f}.")
#             if "top_root_cause" in df.columns and df["top_root_cause"].notna().any():
#                 latest_root = last.get("top_root_cause")
#                 first_root = first.get("top_root_cause")
#                 if pd.notna(latest_root):
#                     if pd.notna(first_root) and latest_root != first_root:
#                         parts.append(f"Root-cause signal shifted from {first_root} to {latest_root}.")
#                     else:
#                         parts.append(f"Latest run root-cause signal: {latest_root}.")
#                 if "root_cause_workload" in df.columns and pd.notna(last.get("root_cause_workload")):
#                     parts.append(f"Latest root-cause workload: {last['root_cause_workload']}.")
#         return " ".join(parts)

#     # Run-over-run comparison output
#     run_cols = {"ingest_run_id", "run_time", "anomaly_count"}
#     if run_cols.issubset(results_df.columns):
#         runs = results_df.sort_values("run_time")
#         first = runs.iloc[0]
#         last = runs.iloc[-1]

#         first_count = int(first["anomaly_count"])
#         last_count = int(last["anomaly_count"])
#         delta = last_count - first_count
#         direction = "increased" if delta > 0 else "decreased" if delta < 0 else "stayed flat"

#         # Only produce yes/no worsening language for explicit trend/worsening questions.
#         trend_keywords = ("getting worse", "wors", "trend", "over time", "improv", "degrad")
#         if any(k in q for k in trend_keywords):
#             if delta > 0:
#                 verdict = "Yes, it is getting worse"
#             elif delta < 0:
#                 verdict = "No, it is not getting worse"
#             else:
#                 verdict = "No clear worsening trend"
#             return (
#                 f"{verdict}. Reason: anomaly_count {first_count} -> {last_count} "
#                 f"across {len(runs)} runs."
#             )

#         # Comparison-style summary for non-trend questions.
#         best_run = runs.loc[runs["anomaly_count"].idxmin()]
#         worst_run = runs.loc[runs["anomaly_count"].idxmax()]
#         return (
#             f"Across {len(runs)} runs, anomaly_count {direction} from {first_count} to {last_count}. "
#             f"Best run was {best_run['ingest_run_id']} with {int(best_run['anomaly_count'])} anomalies, "
#             f"and worst run was {worst_run['ingest_run_id']} with {int(worst_run['anomaly_count'])}."
#         )

#     return None


# def summarize_results(
#     question: str,
#     results_df: pd.DataFrame,
#     use_llm: bool = False,
#     llm_model: str = "llama3.1:8b",
#     llm_host: str = "http://localhost:11434",
# ) -> str:
#     """
#     Summarize query results in plain English.

#     With use_llm=True: passes raw CSV to Ollama for a natural language summary.
#     Fallback: simple row count + column list + first few rows.
#     """
#     if results_df.empty:
#         return "No data matched the query."

#     deterministic = _deterministic_summary(question, results_df)
#     if deterministic:
#         return deterministic

#     if use_llm:
#         try:
#             summary = summarize_results_llm(question, results_df, model=llm_model, host=llm_host)
#             if summary.strip():
#                 return summary.strip()
#         except RuntimeError:
#             pass

#     return _fallback_summary(results_df)
