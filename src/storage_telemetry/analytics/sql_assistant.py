"""
SQL Assistant for storage telemetry analytics.

Responsibilities (only these three):
    1. Translate natural language -> SQL via GitHub Models (three attempts)
    2. Validate SQL is a safe SELECT before execution
    3. Summarize results via GitHub Models

Nothing else. No rule-based fallback. No column-sniffing. No query building.
If the LLM API is unavailable, fail loudly.
"""

import json
import re

import pandas as pd
import requests
from sqlalchemy import text

from storage_telemetry.storage.db_connection import get_engine


FORBIDDEN_KEYWORDS = ("insert", "update", "delete", "drop", "truncate", "alter", "create")
DEFAULT_MODEL = "openai/gpt-4.1"
DEFAULT_API_ENDPOINT = "https://models.github.ai/inference/chat/completions"
DEFAULT_API_VERSION = "2026-03-10"

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SQL_SYSTEM_PROMPT = """
You are a SQL assistant for a storage telemetry analytics platform.
Translate natural language questions into valid PostgreSQL queries.
Return ONLY the raw SQL query — no explanation, no markdown, no backticks, no "SQL:" prefix.

DATABASE SCHEMA:

-- Raw iostat metrics ingested per pipeline run. Each run has a unique ingest_run_id.
CREATE TABLE raw_device_metrics (
    device TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    ingest_run_id TEXT,
    r_s REAL, w_s REAL, rmb_s REAL, wmb_s REAL,
    r_await REAL, w_await REAL, aqu_sz REAL, util_pct REAL,
    rrqm_s REAL, wrqm_s REAL, rareq_sz REAL, wareq_sz REAL,
    svctm REAL, iowait_pct REAL,
    source_file TEXT
);

-- Feature-engineered metrics, one row per device per timestamp per run.
CREATE TABLE curated_device_metrics (
    device TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    ingest_run_id TEXT,
    total_iops REAL, total_throughput_mb_s REAL,
    avg_latency_ms REAL, read_ratio REAL, write_ratio REAL,
    avg_request_size_kb REAL, saturation_score REAL,
    latency_pressure REAL, merge_efficiency REAL,
    queue_efficiency REAL, iowait_pressure REAL,
    workload_pattern TEXT,
    util_pct REAL, aqu_sz REAL,
    hour_of_day INTEGER, day_of_week INTEGER
);

-- One row per anomaly event detected, per device per metric per run.
CREATE TABLE anomaly_events (
    device TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    ingest_run_id TEXT,
    metric_name TEXT,
    metric_value REAL,
    detector_type TEXT,
    anomaly_score REAL,
    severity TEXT,          -- values: 'low', 'medium', 'high', 'critical'
    root_cause_hint TEXT,
    workload_pattern TEXT,
    util_pct REAL, aqu_sz REAL, avg_latency_ms REAL,
    read_ratio REAL, write_ratio REAL,
    avg_request_size_kb REAL, total_iops REAL,
    total_throughput_mb_s REAL, saturation_score REAL,
    latency_pressure REAL
);

-- Aggregated root-cause/workload summary, one row per root_cause+workload per run.
CREATE TABLE mart_tableau_root_cause_summary (
    ingest_run_id TEXT,
    root_cause_hint TEXT,
    workload_pattern TEXT,
    anomaly_count INTEGER,
    critical_count INTEGER,
    high_count INTEGER,
    affected_devices INTEGER,
    avg_anomaly_score REAL
);

-- Pre-aggregated performance + anomaly rollup, one row per device per run.
-- Use this first for historical, trend, comparison, and best/worst device questions.
CREATE TABLE mart_device_run_summary (
    device TEXT,
    ingest_run_id TEXT,
    run_time TIMESTAMP WITH TIME ZONE,
    avg_latency_ms REAL,
    max_latency_ms REAL,
    avg_util_pct REAL,
    max_util_pct REAL,
    avg_iops REAL,
    avg_throughput_mb_s REAL,
    avg_saturation_score REAL,
    avg_aqu_sz REAL,
    dominant_workload_pattern TEXT,
    total_anomalies INTEGER,
    critical_count INTEGER,
    high_count INTEGER,
    top_root_cause TEXT
);

-- Latest run IDs for each dataset.
CREATE VIEW v_latest_run AS
    SELECT
        (SELECT ingest_run_id FROM anomaly_events WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) AS anomaly_run_id,
        (SELECT ingest_run_id FROM curated_device_metrics WHERE ingest_run_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1) AS curated_run_id;

RULES:
1. TABLE SELECTION:
   - mart_device_run_summary: first choice for historical, trend, comparison, best/worst device,
     latency/utilization/saturation/IOPS/throughput questions. Contains top_root_cause per run.
   - curated_device_metrics: timestamp-level drilldowns only (e.g. spikes at a specific time).
   - mart_tableau_root_cause_summary: root-cause rankings, workload vs root-cause distributions.
   - anomaly_events: raw anomaly events, severity breakdowns, detector-level queries.
   - raw_device_metrics: only if the question asks for raw iostat fields (r_s, w_s, r_await, etc.).
   - JOIN tables ON (device, ingest_run_id) to correlate performance with anomalies.

2. Latest run: use v_latest_run.
   - anomaly scope: WHERE ingest_run_id = (SELECT anomaly_run_id FROM v_latest_run)
   - curated/mart scope: WHERE ingest_run_id = (SELECT curated_run_id FROM v_latest_run)

3. Cross-run / trend: no run filter, GROUP BY ingest_run_id, ORDER BY run_time ASC.

4. Text comparisons: always use ILIKE for device, severity, workload_pattern,
   root_cause_hint, metric_name.

5. Severity order: critical=4 > high=3 > medium=2 > low=1.
   CASE WHEN severity ILIKE 'critical' THEN 4 WHEN severity ILIKE 'high' THEN 3
        WHEN severity ILIKE 'medium' THEN 2 ELSE 1 END

6. "Best" / "most stable" = ORDER BY metric ASC (lower latency/util/anomalies = healthier).
   "Worst" = ORDER BY metric DESC.

7. Percentiles: PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY col).

8. All-detector coverage: HAVING COUNT(DISTINCT detector_type) >= 3.

9. Never generate INSERT, UPDATE, DELETE, DROP, TRUNCATE, ALTER, or CREATE.

10. CANNOT_ANSWER only if the question asks about data genuinely absent from all tables
    (e.g. CPU temperature, network bandwidth, cost, memory usage).
    Never use it for trend, cross-run, time-of-day, correlation, ranking, or percentile questions.

EXAMPLES:

Question: Which devices had critical anomalies in the last run?
SQL: SELECT DISTINCT device FROM anomaly_events WHERE ingest_run_id = (SELECT anomaly_run_id FROM v_latest_run) AND severity ILIKE 'critical';

Question: Which device has the highest average utilization in the current run?
SQL: SELECT device, avg_util_pct FROM mart_device_run_summary WHERE ingest_run_id = (SELECT curated_run_id FROM v_latest_run) ORDER BY avg_util_pct DESC;

Question: Historical performance of sda across runs
SQL: SELECT device, ingest_run_id, run_time, avg_latency_ms, avg_util_pct, avg_iops, avg_throughput_mb_s, avg_saturation_score, top_root_cause FROM mart_device_run_summary WHERE device ILIKE 'sda' ORDER BY run_time ASC;

Question: Compare latency and utilization between sda and sdb across all runs
SQL: SELECT device, ingest_run_id, run_time, avg_latency_ms, avg_util_pct FROM mart_device_run_summary WHERE device ILIKE 'sda' OR device ILIKE 'sdb' ORDER BY device, run_time ASC;

Question: Which device performed best across all runs?
SQL: SELECT device, SUM(total_anomalies) AS total_anomalies, ROUND(AVG(avg_latency_ms)::numeric, 2) AS avg_latency FROM mart_device_run_summary GROUP BY device ORDER BY total_anomalies ASC LIMIT 5;

Question: Is sda getting worse over time?
SQL: SELECT ingest_run_id, run_time, total_anomalies, critical_count, avg_latency_ms, avg_saturation_score FROM mart_device_run_summary WHERE device ILIKE 'sda' ORDER BY run_time ASC;

Question: Why is sdb latency high in the latest run?
SQL: SELECT device, avg_latency_ms, avg_util_pct, avg_iops, avg_saturation_score, avg_aqu_sz, top_root_cause, dominant_workload_pattern FROM mart_device_run_summary WHERE ingest_run_id = (SELECT curated_run_id FROM v_latest_run) AND device ILIKE 'sdb';

Question: Top root causes in the latest run?
SQL: SELECT root_cause_hint, workload_pattern, anomaly_count, critical_count, affected_devices FROM mart_tableau_root_cause_summary WHERE ingest_run_id = (SELECT anomaly_run_id FROM v_latest_run) ORDER BY critical_count DESC, anomaly_count DESC LIMIT 10;

Question: Show root-cause trends across runs
SQL: SELECT ingest_run_id, root_cause_hint, workload_pattern, anomaly_count, critical_count FROM mart_tableau_root_cause_summary ORDER BY ingest_run_id, critical_count DESC;

Question: Which devices had high latency AND high queue depth at the same time in the last run?
SQL: SELECT device, timestamp, avg_latency_ms, aqu_sz FROM curated_device_metrics WHERE ingest_run_id = (SELECT curated_run_id FROM v_latest_run) AND avg_latency_ms > 10 AND aqu_sz > 2 ORDER BY avg_latency_ms DESC;

Question: Which devices were flagged by all three detectors in the last run?
SQL: SELECT device, COUNT(DISTINCT detector_type) AS detector_count FROM anomaly_events WHERE ingest_run_id = (SELECT anomaly_run_id FROM v_latest_run) GROUP BY device HAVING COUNT(DISTINCT detector_type) >= 3;

Question: Do anomalies on sdb happen more during business hours or off hours?
SQL: SELECT CASE WHEN hour_of_day BETWEEN 9 AND 17 THEN 'business_hours' ELSE 'off_hours' END AS period, COUNT(*) AS anomaly_count FROM anomaly_events WHERE device ILIKE 'sdb' GROUP BY period ORDER BY anomaly_count DESC;

Question: Are write-heavy workloads more likely to produce critical anomalies than read-heavy ones?
SQL: SELECT workload_pattern, COUNT(*) AS total, SUM(CASE WHEN severity ILIKE 'critical' THEN 1 ELSE 0 END) AS critical_count, ROUND(100.0 * SUM(CASE WHEN severity ILIKE 'critical' THEN 1 ELSE 0 END) / COUNT(*), 1) AS critical_pct FROM anomaly_events WHERE workload_pattern IN ('write_heavy', 'read_heavy') GROUP BY workload_pattern ORDER BY critical_pct DESC;

Question: What is the 95th percentile anomaly score per device?
SQL: SELECT device, ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY anomaly_score)::numeric, 3) AS p95_score FROM anomaly_events GROUP BY device ORDER BY p95_score DESC;

Question: Did critical anomalies increase or decrease between the last two runs?
SQL: WITH runs AS (SELECT ingest_run_id, MIN(run_time) AS run_time, SUM(critical_count) AS critical_count FROM mart_device_run_summary GROUP BY ingest_run_id ORDER BY run_time DESC LIMIT 2) SELECT * FROM runs ORDER BY run_time ASC;

Question: Which single timestamp had the worst combination of latency, queue depth, and utilization?
SQL: SELECT device, timestamp, avg_latency_ms, aqu_sz, util_pct, ROUND((avg_latency_ms * 0.4 + aqu_sz * 30 + util_pct * 0.3)::numeric, 2) AS composite_stress FROM curated_device_metrics ORDER BY composite_stress DESC LIMIT 1;
""".strip()


# ---------------------------------------------------------------------------
# Retry prompts
# ---------------------------------------------------------------------------

NUDGE_SUFFIX = """

IMPORTANT: You must return a valid PostgreSQL SELECT query.
Do not return CANNOT_ANSWER — the schema above has everything needed to answer this.
An imperfect query is better than no answer; it will be validated before execution.
""".strip()

CHAIN_OF_THOUGHT_TEMPLATE = """
Think step by step before writing the SQL:

1. Which table is the best fit?
   - mart_device_run_summary      → historical, trend, comparison, best/worst, per-run performance
   - curated_device_metrics       → timestamp-level drilldowns and spike detection
   - mart_tableau_root_cause_summary → root-cause rankings and workload distributions
   - anomaly_events               → raw anomaly events, severity, detector coverage
   - raw_device_metrics           → only for raw iostat fields (r_s, w_s, r_await, etc.)

2. Latest run or all runs?
   - Latest: WHERE ingest_run_id = (SELECT curated_run_id FROM v_latest_run)
             or   (SELECT anomaly_run_id FROM v_latest_run)
   - All runs / trend: no run filter, ORDER BY run_time ASC

3. What to GROUP BY? (every non-aggregate SELECT column must appear here)

4. What to ORDER BY?
   - Fewer anomalies / lower latency / lower utilization = better health
   - "Best" → ASC, "Worst" → DESC

5. Do I need a CTE for multi-step logic?

Write ONLY the final SQL. No explanation, no markdown, no backticks.

Question: {question}
""".strip()


# ---------------------------------------------------------------------------
# Safety + cleaning
# ---------------------------------------------------------------------------

def _ensure_safe_select(sql: str) -> str:
    stripped = sql.strip().lower()
    if any(kw in stripped for kw in FORBIDDEN_KEYWORDS):
        return "CANNOT_ANSWER"
    if not (stripped.startswith("select") or stripped.startswith("with")):
        return "CANNOT_ANSWER"
    return sql


def _clean_llm_output(text: str) -> str:
    out = text.strip()
    out = re.sub(r"```sql\s*", "", out, flags=re.IGNORECASE)
    out = re.sub(r"```", "", out)
    if out.lower().startswith("sql:"):
        out = out[4:]
    return out.strip()


# ---------------------------------------------------------------------------
# GitHub Models transport
# ---------------------------------------------------------------------------

def _call_llm(
    messages: list[dict],
    model: str = DEFAULT_MODEL,
    api_key: str = None,
    api_endpoint: str = DEFAULT_API_ENDPOINT,
    timeout: int = 45,
) -> str:
    """POST a chat request to GitHub Models. Raises RuntimeError if unreachable."""
    if not api_key:
        raise RuntimeError(
            "LLM API key not provided. Set via --api-key or LLM_API_KEY env var. "
            "For GitHub Models, use a fine-grained GitHub PAT with models:read."
        )

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "X-GitHub-Api-Version": DEFAULT_API_VERSION,
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0,
    }
    try:
        response = requests.post(
            api_endpoint,
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        if response.status_code == 401:
            raise RuntimeError(
                "GitHub Models authentication failed (401). "
                "Use a fine-grained GitHub PAT with models:read permission."
            )
        response.raise_for_status()
        body = response.json()
        return body.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
    except RuntimeError:
        raise
    except (requests.RequestException, KeyError, ValueError) as exc:
        raise RuntimeError(f"LLM API unreachable: {exc}") from exc


def _attempt(
    system: str,
    user: str,
    model: str,
    api_key: str,
    api_endpoint: str,
) -> str:
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]
    raw = _call_llm(messages, model=model, api_key=api_key, api_endpoint=api_endpoint)
    sql = _clean_llm_output(raw)
    if sql.upper() == "CANNOT_ANSWER":
        return "CANNOT_ANSWER"
    return _ensure_safe_select(sql)


# ---------------------------------------------------------------------------
# SQL generation — three attempts
# ---------------------------------------------------------------------------

def generate_sql(
    question: str,
    model: str = DEFAULT_MODEL,
    api_key: str = None,
    api_endpoint: str = DEFAULT_API_ENDPOINT,
) -> str:
    """
    Translate a natural language question into a safe PostgreSQL SELECT query.

    Three attempts:
      1. Standard prompt — handles the majority of questions.
      2. Nudge — tells the model not to give up.
      3. Chain-of-thought — step-by-step reasoning for complex queries.

    Raises RuntimeError if LLM API is unreachable (fail loudly, no silent fallback).
    Returns 'CANNOT_ANSWER' only if all three attempts genuinely cannot produce a query.
    """
    sql = _attempt(SQL_SYSTEM_PROMPT, f"Question: {question}", model, api_key, api_endpoint)
    if sql != "CANNOT_ANSWER":
        return sql

    sql = _attempt(
        SQL_SYSTEM_PROMPT + "\n\n" + NUDGE_SUFFIX,
        f"Question: {question}",
        model,
        api_key,
        api_endpoint,
    )
    if sql != "CANNOT_ANSWER":
        return sql

    return _attempt(
        SQL_SYSTEM_PROMPT,
        CHAIN_OF_THOUGHT_TEMPLATE.format(question=question),
        model,
        api_key,
        api_endpoint,
    )


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def run_query(sql: str) -> pd.DataFrame:
    """Execute a validated SELECT query and return results as a DataFrame."""
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


# ---------------------------------------------------------------------------
# Result summarization
# ---------------------------------------------------------------------------

SUMMARIZE_SYSTEM_PROMPT = """
You are a data analyst assistant for a storage telemetry platform.
Summarize the query results in 2-3 plain English sentences.
Be specific: include device names, metric values, and numbers from the results.
If results show a trend across runs, describe the direction: improving, degrading, or stable.
Do not mention SQL, table names, or column names.
""".strip()


def summarize_results(
    question: str,
    results_df: pd.DataFrame,
    model: str = DEFAULT_MODEL,
    api_key: str = None,
    api_endpoint: str = DEFAULT_API_ENDPOINT,
) -> str:
    """
    Summarize query results in plain English via LLM API.
    Raises RuntimeError if LLM API is unavailable (fail loudly).
    """
    if results_df.empty:
        return "No data matched the query."

    csv_preview = results_df.head(200).to_csv(index=False)
    messages = [
        {"role": "system", "content": SUMMARIZE_SYSTEM_PROMPT},
        {"role": "user", "content": f"Question: {question}\n\nResults:\n{csv_preview}"},
    ]
    summary = _call_llm(messages, model=model, api_key=api_key, api_endpoint=api_endpoint)
    return summary.strip() or "No summary returned by model."
