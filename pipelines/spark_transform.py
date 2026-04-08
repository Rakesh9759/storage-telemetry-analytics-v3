"""Spark transform pipeline for curated storage metrics (with pandas fallback)."""

import argparse
import os

import pandas as pd
import yaml

from storage_telemetry.storage.db_connection import get_engine
from storage_telemetry.storage.db_store import write_to_db


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for transform stage."""
    parser = argparse.ArgumentParser(description="Transform raw metrics into curated metrics")
    parser.add_argument("--ingest-run-id", default=None, help="Optional ingest run ID to process")
    return parser.parse_args()


def load_db_config(config_path: str) -> dict:
    """Load postgres connection settings from YAML config."""
    with open(config_path, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    return config.get("database", {}).get("postgres", config.get("postgres", {}))


def get_latest_raw_ingest_run_id() -> str | None:
    """Get the latest ingest_run_id present in raw_device_metrics."""
    engine = get_engine()
    latest = pd.read_sql(
        """
        SELECT ingest_run_id
        FROM raw_device_metrics
        WHERE ingest_run_id IS NOT NULL
        ORDER BY ctid DESC
        LIMIT 1
        """,
        engine,
    )
    if latest.empty:
        return None
    return str(latest.iloc[0, 0])


def ensure_columns_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure expected columns exist for pandas fallback path."""
    legacy_name_map = {
        "r/s": "r_s",
        "w/s": "w_s",
        "rMB/s": "rmb_s",
        "wMB/s": "wmb_s",
        "rrqm/s": "rrqm_s",
        "wrqm/s": "wrqm_s",
    }
    df = df.rename(columns={k: v for k, v in legacy_name_map.items() if k in df.columns and v not in df.columns})

    defaults = {
        "r_s": 0.0,
        "w_s": 0.0,
        "rmb_s": 0.0,
        "wmb_s": 0.0,
        "r_await": 0.0,
        "w_await": 0.0,
        "aqu_sz": 0.0,
        "util_pct": 0.0,
        "rrqm_s": 0.0,
        "wrqm_s": 0.0,
        "rareq_sz": 0.0,
        "wareq_sz": 0.0,
        "svctm": 0.0,
        "iowait_pct": 0.0,
    }
    for col_name, default_value in defaults.items():
        if col_name not in df.columns:
            df[col_name] = default_value

    if "source_file" not in df.columns:
        df["source_file"] = None
    if "ingest_run_id" not in df.columns:
        df["ingest_run_id"] = None

    return df


def feature_engineering_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Compute curated fields using pandas when Spark runtime is unavailable."""
    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")

    total_iops = out["r_s"] + out["w_s"]
    total_throughput = out["rmb_s"] + out["wmb_s"]

    out["total_iops"] = total_iops
    out["iops_total"] = total_iops
    out["total_throughput_mb_s"] = total_throughput
    out["throughput_mb_s"] = total_throughput

    out["avg_latency_ms"] = 0.0
    nonzero_iops = total_iops > 0
    out.loc[nonzero_iops, "avg_latency_ms"] = (
        (out.loc[nonzero_iops, "r_await"] * out.loc[nonzero_iops, "r_s"])
        + (out.loc[nonzero_iops, "w_await"] * out.loc[nonzero_iops, "w_s"])
    ) / total_iops[nonzero_iops]
    out["weighted_avg_latency"] = out["avg_latency_ms"]

    out["read_ratio"] = 0.5
    out["write_ratio"] = 0.5
    out.loc[nonzero_iops, "read_ratio"] = out.loc[nonzero_iops, "r_s"] / total_iops[nonzero_iops]
    out.loc[nonzero_iops, "write_ratio"] = out.loc[nonzero_iops, "w_s"] / total_iops[nonzero_iops]

    out["avg_request_size_kb"] = 0.0
    out.loc[nonzero_iops, "avg_request_size_kb"] = (total_throughput[nonzero_iops] * 1024.0) / total_iops[nonzero_iops]

    out["saturation_score"] = out["util_pct"] * out["aqu_sz"]
    out["io_intensity"] = out["total_iops"] * out["avg_request_size_kb"]
    out["latency_pressure"] = out["avg_latency_ms"] * out["aqu_sz"]

    out["merge_rate_total"] = out["rrqm_s"] + out["wrqm_s"]
    denom_merge = out["total_iops"] + out["merge_rate_total"]
    out["merge_efficiency"] = 0.0
    nonzero_merge = denom_merge > 0
    out.loc[nonzero_merge, "merge_efficiency"] = out.loc[nonzero_merge, "merge_rate_total"] / denom_merge[nonzero_merge]

    out["await_ratio"] = 1.0
    nonzero_rawait = out["r_await"] > 0
    out.loc[nonzero_rawait, "await_ratio"] = out.loc[nonzero_rawait, "w_await"] / out.loc[nonzero_rawait, "r_await"]

    out["svctm_await_ratio"] = 1.0
    nonzero_lat = out["avg_latency_ms"] > 0
    out.loc[nonzero_lat, "svctm_await_ratio"] = out.loc[nonzero_lat, "svctm"] / out.loc[nonzero_lat, "avg_latency_ms"]

    out["queue_efficiency"] = 0.0
    nonzero_aqu = out["aqu_sz"] > 0
    out.loc[nonzero_aqu, "queue_efficiency"] = out.loc[nonzero_aqu, "total_iops"] / out.loc[nonzero_aqu, "aqu_sz"]

    out["write_amplification"] = 1.0
    nonzero_rs = out["r_s"] > 0
    out.loc[nonzero_rs, "write_amplification"] = out.loc[nonzero_rs, "w_s"] / out.loc[nonzero_rs, "r_s"]

    out["iowait_pressure"] = (out["iowait_pct"] * out["util_pct"]) / 100.0

    out["hour_of_day"] = out["timestamp"].dt.hour
    out["day_of_week"] = out["timestamp"].dt.dayofweek

    out["workload_pattern"] = "balanced"
    out.loc[(out["util_pct"] > 80) | (out["aqu_sz"] > 5), "workload_pattern"] = "saturated"
    out.loc[out["total_iops"] > 1200, "workload_pattern"] = "burst_io"
    out.loc[out["avg_request_size_kb"] < 16, "workload_pattern"] = "small_io_pressure"
    out.loc[out["avg_latency_ms"] > 5, "workload_pattern"] = "latency_sensitive"

    return out


def ensure_columns(df):
    """Ensure expected raw columns exist so transforms are resilient to schema drift."""
    from pyspark.sql.functions import lit

    legacy_name_map = {
        "r/s": "r_s",
        "w/s": "w_s",
        "rMB/s": "rmb_s",
        "wMB/s": "wmb_s",
        "rrqm/s": "rrqm_s",
        "wrqm/s": "wrqm_s",
    }
    for old_name, new_name in legacy_name_map.items():
        if old_name in df.columns and new_name not in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    defaults = {
        "r_s": 0.0,
        "w_s": 0.0,
        "rmb_s": 0.0,
        "wmb_s": 0.0,
        "r_await": 0.0,
        "w_await": 0.0,
        "aqu_sz": 0.0,
        "util_pct": 0.0,
        "rrqm_s": 0.0,
        "wrqm_s": 0.0,
        "rareq_sz": 0.0,
        "wareq_sz": 0.0,
        "svctm": 0.0,
        "iowait_pct": 0.0,
    }
    for col_name, default_value in defaults.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(default_value))

    if "source_file" not in df.columns:
        df = df.withColumn("source_file", lit(None).cast("string"))
    if "ingest_run_id" not in df.columns:
        df = df.withColumn("ingest_run_id", lit(None).cast("string"))

    return df


def feature_engineering(df):
    """Compute curated fields expected by marts and anomaly scripts."""
    from pyspark.sql.functions import col, dayofweek, hour, lit, to_timestamp, when

    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

    total_iops_expr = col("r_s") + col("w_s")
    total_throughput_expr = col("rmb_s") + col("wmb_s")

    df = df.withColumn("total_iops", total_iops_expr)
    df = df.withColumn("iops_total", total_iops_expr)
    df = df.withColumn("total_throughput_mb_s", total_throughput_expr)
    df = df.withColumn("throughput_mb_s", total_throughput_expr)

    df = df.withColumn(
        "avg_latency_ms",
        when(
            total_iops_expr > 0,
            ((col("r_await") * col("r_s")) + (col("w_await") * col("w_s"))) / total_iops_expr,
        ).otherwise(lit(0.0)),
    )
    df = df.withColumn("weighted_avg_latency", col("avg_latency_ms"))

    df = df.withColumn(
        "read_ratio",
        when(total_iops_expr > 0, col("r_s") / total_iops_expr).otherwise(lit(0.5)),
    )
    df = df.withColumn(
        "write_ratio",
        when(total_iops_expr > 0, col("w_s") / total_iops_expr).otherwise(lit(0.5)),
    )
    df = df.withColumn(
        "avg_request_size_kb",
        when(total_iops_expr > 0, (total_throughput_expr * lit(1024.0)) / total_iops_expr).otherwise(lit(0.0)),
    )

    df = df.withColumn("saturation_score", col("util_pct") * col("aqu_sz"))
    df = df.withColumn("io_intensity", col("total_iops") * col("avg_request_size_kb"))
    df = df.withColumn("latency_pressure", col("avg_latency_ms") * col("aqu_sz"))

    df = df.withColumn("merge_rate_total", col("rrqm_s") + col("wrqm_s"))
    df = df.withColumn(
        "merge_efficiency",
        when(
            (col("total_iops") + col("merge_rate_total")) > 0,
            col("merge_rate_total") / (col("total_iops") + col("merge_rate_total")),
        ).otherwise(lit(0.0)),
    )
    df = df.withColumn(
        "await_ratio",
        when(col("r_await") > 0, col("w_await") / col("r_await")).otherwise(lit(1.0)),
    )
    df = df.withColumn(
        "svctm_await_ratio",
        when(col("avg_latency_ms") > 0, col("svctm") / col("avg_latency_ms")).otherwise(lit(1.0)),
    )
    df = df.withColumn(
        "queue_efficiency",
        when(col("aqu_sz") > 0, col("total_iops") / col("aqu_sz")).otherwise(lit(0.0)),
    )
    df = df.withColumn(
        "write_amplification",
        when(col("r_s") > 0, col("w_s") / col("r_s")).otherwise(lit(1.0)),
    )
    df = df.withColumn("iowait_pressure", (col("iowait_pct") * col("util_pct")) / lit(100.0))

    df = df.withColumn("hour_of_day", hour(col("timestamp")))
    df = df.withColumn("day_of_week", dayofweek(col("timestamp")) - lit(1))

    df = df.withColumn(
        "workload_pattern",
        when((col("util_pct") > 80) | (col("aqu_sz") > 5), lit("saturated"))
        .when(col("total_iops") > 1200, lit("burst_io"))
        .when(col("avg_request_size_kb") < 16, lit("small_io_pressure"))
        .when(col("avg_latency_ms") > 5, lit("latency_sensitive"))
        .otherwise(lit("balanced")),
    )

    return df


def fetch_latest_raw_ingest_run_id() -> str | None:
    """Return the latest ingest_run_id from raw_device_metrics based on newest row."""
    engine = get_engine()
    query = """
        SELECT ingest_run_id
        FROM raw_device_metrics
        WHERE ingest_run_id IS NOT NULL
        ORDER BY ctid DESC
        LIMIT 1
    """
    result = pd.read_sql(query, engine)
    if result.empty:
        return None
    return result.iloc[0, 0]


def main() -> None:
    """Run Spark transform from raw_device_metrics to curated_device_metrics.

    Falls back to pandas transform when Spark runtime is unavailable.
    """
    args = parse_args()
    db = load_db_config(os.path.join("configs", "database.yaml"))
    jdbc_url = f"jdbc:postgresql://{db['host']}:{db['port']}/{db['db']}"
    properties = {"user": db["user"], "password": db["password"], "driver": "org.postgresql.Driver"}
    latest_ingest_run_id = args.ingest_run_id or fetch_latest_raw_ingest_run_id()

    if latest_ingest_run_id:
        safe_run_id = str(latest_ingest_run_id).replace("'", "''")
        source_query = (
            "(SELECT * FROM raw_device_metrics "
            f"WHERE ingest_run_id = '{safe_run_id}') AS raw_device_metrics_latest"
        )
        pandas_query = f"SELECT * FROM raw_device_metrics WHERE ingest_run_id = '{safe_run_id}'"
        print(f"Transform scope ingest_run_id={latest_ingest_run_id}")
    else:
        source_query = "raw_device_metrics"
        pandas_query = "SELECT * FROM raw_device_metrics"
        print("Transform scope ingest_run_id=<none>; using full raw_device_metrics table")

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("StorageTelemetrySpark").getOrCreate()
        df_raw = spark.read.jdbc(url=jdbc_url, table=source_query, properties=properties)
        df_raw = ensure_columns(df_raw)
        df_curated = feature_engineering(df_raw)
        if latest_ingest_run_id:
            # Delete this run's existing rows before appending to avoid duplicates
            with get_engine().begin() as conn:
                conn.execute(
                    __import__("sqlalchemy").text(
                        "DELETE FROM curated_device_metrics WHERE ingest_run_id = :run_id"
                    ),
                    {"run_id": latest_ingest_run_id},
                )
        (
            df_curated.write.mode("append")
            .jdbc(jdbc_url, "curated_device_metrics", properties)
        )
        spark.stop()
        print("Spark transform completed.")
        return
    except Exception as exc:
        print(f"Spark unavailable, using pandas fallback: {exc}")

    engine = get_engine()
    raw_df = pd.read_sql(pandas_query, engine)
    raw_df = ensure_columns_pandas(raw_df)
    curated_df = feature_engineering_pandas(raw_df)
    if latest_ingest_run_id:
        # Delete this run's existing rows before appending to avoid duplicates
        with engine.begin() as conn:
            conn.execute(
                __import__("sqlalchemy").text(
                    "DELETE FROM curated_device_metrics WHERE ingest_run_id = :run_id"
                ),
                {"run_id": latest_ingest_run_id},
            )
    write_to_db(curated_df, "curated_device_metrics", if_exists="append")
    print("Pandas fallback transform completed.")


if __name__ == "__main__":
    main()
