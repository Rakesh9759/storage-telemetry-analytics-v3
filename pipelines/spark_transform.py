"""Spark transform pipeline for curated storage telemetry metrics (Postgres source)."""

import os

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, lit, to_timestamp, when


def load_db_config(config_path: str) -> dict:
    """Load postgres connection settings from YAML config."""
    with open(config_path, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    return config["postgres"]


def ensure_columns(df):
    """Ensure expected raw columns exist so transforms are resilient to schema drift."""
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


def main() -> None:
    """Run Spark transform from raw_device_metrics to curated_device_metrics."""
    db = load_db_config(os.path.join("configs", "database.yaml"))
    spark = SparkSession.builder.appName("StorageTelemetrySpark").getOrCreate()

    jdbc_url = f"jdbc:postgresql://{db['host']}:{db['port']}/{db['db']}"
    properties = {
        "user": db["user"],
        "password": db["password"],
        "driver": "org.postgresql.Driver",
    }

    df_raw = spark.read.jdbc(url=jdbc_url, table="raw_device_metrics", properties=properties)
    df_raw = ensure_columns(df_raw)
    df_curated = feature_engineering(df_raw)

    df_curated.write.mode("overwrite").jdbc(jdbc_url, "curated_device_metrics", properties)
    spark.stop()


if __name__ == "__main__":
    main()
