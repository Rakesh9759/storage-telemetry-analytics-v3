import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone


# ---------------------------------------------------------------------------
# Device profiles — realistic storage device archetypes
# ---------------------------------------------------------------------------
# Each profile now includes:
#   - rrqm_s / wrqm_s  : merge rates (requests/s merged before hitting disk)
#   - rareq_sz / wareq_sz : average request sizes (KB), separate for R and W
#   - svctm             : actual service time (ms), excludes queue wait
#   - iowait_pct        : CPU % stalled waiting on I/O
# ---------------------------------------------------------------------------
DEVICE_PROFILES = {
    "nvme0n1": {
        "type": "NVMe SSD (OS/boot)",
        "r_s":       (300, 60),
        "w_s":       (150, 30),
        "rmb_s":     (25, 5),
        "wmb_s":     (12, 3),
        "r_await":   (0.3, 0.15),
        "w_await":   (0.5, 0.2),
        "aqu_sz":    (0.4, 0.15),
        "util_pct":  (25, 8),
        # new raw fields
        "rrqm_s":    (12, 4),       # NVMe merges less (already fast)
        "wrqm_s":    (8, 3),
        "rareq_sz":  (64, 20),      # KB — mixed small/large reads
        "wareq_sz":  (32, 12),      # KB — writes tend to be smaller
        "svctm":     (0.15, 0.08),  # ms — very fast service time
        "iowait_pct": (1.5, 0.8),   # CPU rarely stalled on NVMe
    },
    "nvme1n1": {
        "type": "NVMe SSD (database)",
        "r_s":       (800, 150),
        "w_s":       (600, 120),
        "rmb_s":     (50, 10),
        "wmb_s":     (40, 8),
        "r_await":   (0.2, 0.1),
        "w_await":   (0.4, 0.15),
        "aqu_sz":    (1.5, 0.5),
        "util_pct":  (55, 12),
        # new raw fields
        "rrqm_s":    (30, 8),       # database generates more mergeable sequential reads
        "wrqm_s":    (25, 7),
        "rareq_sz":  (16, 6),       # DB random I/O — small reads (4-16KB pages)
        "wareq_sz":  (8, 3),        # WAL/journal writes are small
        "svctm":     (0.1, 0.05),
        "iowait_pct": (3.0, 1.2),   # DB workloads push CPU iowait higher
    },
    "sda": {
        "type": "SATA SSD (app tier)",
        "r_s":       (200, 50),
        "w_s":       (180, 40),
        "rmb_s":     (15, 4),
        "wmb_s":     (12, 3),
        "r_await":   (1.0, 0.4),
        "w_await":   (1.5, 0.5),
        "aqu_sz":    (1.0, 0.3),
        "util_pct":  (45, 10),
        # new raw fields
        "rrqm_s":    (20, 6),
        "wrqm_s":    (15, 5),
        "rareq_sz":  (48, 16),
        "wareq_sz":  (40, 14),
        "svctm":     (0.6, 0.25),
        "iowait_pct": (4.0, 1.5),
    },
    "sdb": {
        "type": "HDD (bulk storage/logs)",
        "r_s":       (80, 25),
        "w_s":       (120, 35),
        "rmb_s":     (8, 3),
        "wmb_s":     (10, 3),
        "r_await":   (5.0, 2.0),
        "w_await":   (7.0, 2.5),
        "aqu_sz":    (2.5, 0.8),
        "util_pct":  (65, 12),
        # new raw fields
        "rrqm_s":    (50, 15),      # HDD benefits most from merging (seek cost)
        "wrqm_s":    (60, 18),
        "rareq_sz":  (128, 40),     # sequential bulk reads are large
        "wareq_sz":  (96, 30),
        "svctm":     (4.0, 1.5),    # ms — seek + rotational latency
        "iowait_pct": (12.0, 4.0),  # HDD stalls CPUs much more
    },
    "dm-0": {
        "type": "LVM/Device mapper (RAID)",
        "r_s":       (400, 80),
        "w_s":       (350, 70),
        "rmb_s":     (30, 6),
        "wmb_s":     (25, 5),
        "r_await":   (0.8, 0.3),
        "w_await":   (1.2, 0.4),
        "aqu_sz":    (1.8, 0.6),
        "util_pct":  (50, 10),
        # new raw fields
        "rrqm_s":    (35, 10),
        "wrqm_s":    (30, 9),
        "rareq_sz":  (72, 22),
        "wareq_sz":  (60, 18),
        "svctm":     (0.4, 0.15),
        "iowait_pct": (5.0, 2.0),
    },
}

# ---------------------------------------------------------------------------
# Anomaly scenarios — now scale relative to each device's baseline profile
# so a latency spike on NVMe is proportionally extreme vs. HDD baseline.
# ---------------------------------------------------------------------------
ANOMALY_SCENARIOS = [
    {
        "name": "disk_saturation",
        "fields": {
            "util_pct":  {"mode": "abs",  "val": (95, 3)},
            "aqu_sz":    {"mode": "mul",  "val": 6.0},
            "r_await":   {"mode": "mul",  "val": 8.0},
            "w_await":   {"mode": "mul",  "val": 10.0},
            "svctm":     {"mode": "mul",  "val": 5.0},
            "iowait_pct":{"mode": "abs",  "val": (35, 8)},
        },
    },
    {
        "name": "iops_burst",
        "fields": {
            "r_s":       {"mode": "mul",  "val": 5.0},
            "w_s":       {"mode": "mul",  "val": 4.0},
            "rrqm_s":    {"mode": "mul",  "val": 4.0},   # merges rise with burst
            "wrqm_s":    {"mode": "mul",  "val": 4.0},
            "iowait_pct":{"mode": "mul",  "val": 3.0},
        },
    },
    {
        "name": "latency_spike",
        "fields": {
            "r_await":   {"mode": "mul",  "val": 20.0},
            "w_await":   {"mode": "mul",  "val": 25.0},
            "aqu_sz":    {"mode": "mul",  "val": 5.0},
            "svctm":     {"mode": "mul",  "val": 15.0},
            "iowait_pct":{"mode": "mul",  "val": 6.0},
        },
    },
    {
        "name": "write_storm",
        "fields": {
            "w_s":       {"mode": "mul",  "val": 8.0},
            "wmb_s":     {"mode": "mul",  "val": 6.0},
            "w_await":   {"mode": "mul",  "val": 12.0},
            "wrqm_s":    {"mode": "mul",  "val": 5.0},
            "wareq_sz":  {"mode": "mul",  "val": 2.0},   # write size balloons
            "iowait_pct":{"mode": "mul",  "val": 4.0},
        },
    },
    {
        "name": "read_flood",
        "fields": {
            "r_s":       {"mode": "mul",  "val": 6.0},
            "rmb_s":     {"mode": "mul",  "val": 5.0},
            "r_await":   {"mode": "mul",  "val": 8.0},
            "rrqm_s":    {"mode": "mul",  "val": 3.5},
            "rareq_sz":  {"mode": "mul",  "val": 1.8},
            "iowait_pct":{"mode": "mul",  "val": 3.5},
        },
    },
    {
        "name": "queue_buildup",
        "fields": {
            "aqu_sz":    {"mode": "mul",  "val": 8.0},
            "util_pct":  {"mode": "abs",  "val": (92, 4)},
            "r_await":   {"mode": "mul",  "val": 6.0},
            "w_await":   {"mode": "mul",  "val": 8.0},
            "svctm":     {"mode": "mul",  "val": 4.0},
            "iowait_pct":{"mode": "abs",  "val": (28, 6)},
        },
    },
    {
        # New: merge rate collapse — device stops merging under fragmentation/pressure
        "name": "merge_collapse",
        "fields": {
            "rrqm_s":    {"mode": "abs",  "val": (0.5, 0.3)},
            "wrqm_s":    {"mode": "abs",  "val": (0.5, 0.3)},
            "r_await":   {"mode": "mul",  "val": 4.0},   # latency rises as merges drop
            "w_await":   {"mode": "mul",  "val": 5.0},
            "rareq_sz":  {"mode": "abs",  "val": (4, 1)},  # requests shrink (random I/O)
            "wareq_sz":  {"mode": "abs",  "val": (4, 1)},
        },
    },
    {
        # New: small IO storm — tiny random requests hammer the device
        "name": "small_io_storm",
        "fields": {
            "r_s":       {"mode": "mul",  "val": 4.0},
            "w_s":       {"mode": "mul",  "val": 4.0},
            "rareq_sz":  {"mode": "abs",  "val": (4, 1)},
            "wareq_sz":  {"mode": "abs",  "val": (4, 1)},
            "r_await":   {"mode": "mul",  "val": 5.0},
            "w_await":   {"mode": "mul",  "val": 6.0},
            "aqu_sz":    {"mode": "mul",  "val": 4.0},
            "iowait_pct":{"mode": "mul",  "val": 5.0},
        },
    },
]


def _time_of_day_factor(hour, day_of_week):
    """Business-hours load pattern: peaks 9-17 on weekdays, lower on weekends."""
    base = 0.4 if day_of_week >= 5 else 1.0
    hourly = 0.3 + 0.7 * np.exp(-0.5 * ((hour - 13) / 4) ** 2)
    return base * hourly


def _apply_anomaly(row, scenario):
    """
    Apply anomaly scenario to a row. Uses multiplicative scaling against the
    current baseline value so thresholds are proportional to device archetype.
    """
    noise = lambda: np.random.uniform(0.85, 1.15)
    for field, spec in scenario["fields"].items():
        if field not in row:
            continue
        if spec["mode"] == "mul":
            row[field] = abs(row[field]) * spec["val"] * noise()
        else:  # abs
            mu, sigma = spec["val"]
            row[field] = max(np.random.normal(mu, sigma), 0)
    return row


def _derive_curated_fields(df):
    """
    Build all curated/derived features on top of the raw metrics.

    New fields added beyond original:
      - throughput_mb_s       : total bandwidth (rmb_s + wmb_s)
      - iops_total            : total IOPS (r_s + w_s)
      - merge_rate_total      : rrqm_s + wrqm_s
      - merge_efficiency      : merges / (iops + merges) — how much work is being saved
      - await_ratio           : r_await / w_await — latency asymmetry signal
      - svctm_await_ratio     : svctm / avg_await — queue overhead ratio (>1 = bad)
      - queue_efficiency      : iops_total / aqu_sz — throughput per queued request
      - write_amplification   : w_s / r_s — write-heavy imbalance
      - iowait_pressure       : iowait_pct * util_pct — combined CPU+device stress
      (existing fields retained: avg_latency_ms, read_ratio, write_ratio,
       avg_request_size_kb, saturation_score, io_intensity, latency_pressure,
       hour_of_day, day_of_week, workload_pattern)
    """
    df = df.copy()

    # --- Existing derived fields ---
    df["avg_latency_ms"] = (
        df["r_await"] * df["r_s"] + df["w_await"] * df["w_s"]
    ) / (df["r_s"] + df["w_s"]).replace(0, np.nan)
    df["avg_latency_ms"] = df["avg_latency_ms"].fillna(0)

    total_io = df["r_s"] + df["w_s"]
    df["read_ratio"]  = df["r_s"]  / total_io.replace(0, np.nan).fillna(0.5)
    df["write_ratio"] = df["w_s"]  / total_io.replace(0, np.nan).fillna(0.5)

    df["avg_request_size_kb"] = (
        (df["rmb_s"] * 1024 + df["wmb_s"] * 1024) / total_io.replace(0, np.nan)
    ).fillna(0)

    df["saturation_score"] = df["util_pct"] * df["aqu_sz"]
    df["io_intensity"]     = total_io * df["avg_request_size_kb"]
    df["latency_pressure"] = df["avg_latency_ms"] * df["aqu_sz"]

    df["hour_of_day"]  = pd.to_datetime(df["timestamp"]).dt.hour
    df["day_of_week"]  = pd.to_datetime(df["timestamp"]).dt.dayofweek

    # Workload pattern classification
    conditions = [
        (df["util_pct"] > 80) | (df["aqu_sz"] > 5),
        (df["r_s"] > df["r_s"].quantile(0.9)) | (df["w_s"] > df["w_s"].quantile(0.9)),
        df["avg_request_size_kb"] < 16,
        df["avg_latency_ms"] > df["avg_latency_ms"].quantile(0.9),
    ]
    choices = ["saturated", "burst_io", "small_io_pressure", "latency_sensitive"]
    df["workload_pattern"] = np.select(conditions, choices, default="balanced")

    # --- New derived fields ---
    df["throughput_mb_s"]   = df["rmb_s"] + df["wmb_s"]
    df["iops_total"]        = total_io

    df["merge_rate_total"]  = df["rrqm_s"] + df["wrqm_s"]
    # merge efficiency: what fraction of potential IOPS were merged away
    effective_iops = df["iops_total"] + df["merge_rate_total"]
    df["merge_efficiency"]  = (
        df["merge_rate_total"] / effective_iops.replace(0, np.nan)
    ).fillna(0).clip(0, 1)

    # Latency asymmetry: how much worse writes are vs reads (or vice versa)
    df["await_ratio"] = (
        df["w_await"] / df["r_await"].replace(0, np.nan)
    ).fillna(1.0)

    # Queue overhead: svctm/avg_await tells us how much time is pure queue wait
    # Values close to 1 = no queuing overhead; < 0.5 = heavy queue saturation
    df["svctm_await_ratio"] = (
        df["svctm"] / df["avg_latency_ms"].replace(0, np.nan)
    ).fillna(1.0).clip(0, 10)

    # Queue efficiency: IOPS delivered per unit of queue depth
    df["queue_efficiency"] = (
        df["iops_total"] / df["aqu_sz"].replace(0, np.nan)
    ).fillna(0)

    # Write amplification: write IOPS relative to read IOPS
    df["write_amplification"] = (
        df["w_s"] / df["r_s"].replace(0, np.nan)
    ).fillna(1.0)

    # Combined CPU + device stress (iowait × utilization)
    df["iowait_pressure"] = df["iowait_pct"] * df["util_pct"] / 100.0

    return df


def generate_iostat_like_data(days=7, interval_minutes=5):
    """
    Generate realistic storage telemetry data.

    Args:
        days: Number of days to simulate (default 7 -> ~2000 samples/device)
        interval_minutes: Sampling interval in minutes
    """
    samples_per_device = int((days * 24 * 60) / interval_minutes)
    rows = []

    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    for device, profile in DEVICE_PROFILES.items():
        # Per-device drift: gradual degradation over time for some devices
        has_degradation  = np.random.random() < 0.3
        degradation_rate = np.random.uniform(0.001, 0.003) if has_degradation else 0

        # Anomaly windows: 2-5 bursts per device
        num_anomaly_windows = np.random.randint(2, 6)
        anomaly_starts      = sorted(np.random.randint(0, samples_per_device, num_anomaly_windows))
        anomaly_durations   = np.random.randint(3, 15, num_anomaly_windows)
        anomaly_set         = {}
        for a_start, a_dur in zip(anomaly_starts, anomaly_durations):
            scenario = ANOMALY_SCENARIOS[np.random.randint(len(ANOMALY_SCENARIOS))]
            for j in range(a_dur):
                anomaly_set[a_start + j] = scenario

        for i in range(samples_per_device):
            jitter = timedelta(seconds=np.random.uniform(-30, 30))
            ts     = start + timedelta(minutes=i * interval_minutes) + jitter

            hour        = ts.hour
            day_of_week = ts.weekday()
            load        = _time_of_day_factor(hour, day_of_week)
            drift       = 1.0 + degradation_rate * i

            row = {
                "device":    device,
                "timestamp": ts.isoformat(),
                # --- existing fields ---
                "r_s":     max(np.random.normal(profile["r_s"][0],    profile["r_s"][1])    * load * drift, 0),
                "w_s":     max(np.random.normal(profile["w_s"][0],    profile["w_s"][1])    * load * drift, 0),
                "rmb_s":   max(np.random.normal(profile["rmb_s"][0],  profile["rmb_s"][1])  * load, 0),
                "wmb_s":   max(np.random.normal(profile["wmb_s"][0],  profile["wmb_s"][1])  * load, 0),
                "r_await": max(np.random.lognormal(np.log(profile["r_await"][0] * drift),   profile["r_await"][1]), 0.01),
                "w_await": max(np.random.lognormal(np.log(profile["w_await"][0] * drift),   profile["w_await"][1]), 0.01),
                "aqu_sz":  max(np.random.lognormal(np.log(profile["aqu_sz"][0]),             profile["aqu_sz"][1])  * load, 0),
                "util_pct": np.clip(np.random.normal(profile["util_pct"][0] * load * drift, profile["util_pct"][1]), 0, 100),
                # --- new raw fields ---
                "rrqm_s":   max(np.random.normal(profile["rrqm_s"][0],   profile["rrqm_s"][1])   * load, 0),
                "wrqm_s":   max(np.random.normal(profile["wrqm_s"][0],   profile["wrqm_s"][1])   * load, 0),
                "rareq_sz": max(np.random.normal(profile["rareq_sz"][0], profile["rareq_sz"][1]), 1.0),
                "wareq_sz": max(np.random.normal(profile["wareq_sz"][0], profile["wareq_sz"][1]), 1.0),
                "svctm":    max(np.random.lognormal(np.log(profile["svctm"][0] * drift),    profile["svctm"][1]), 0.001),
                "iowait_pct": np.clip(np.random.normal(profile["iowait_pct"][0] * load * drift, profile["iowait_pct"][1]), 0, 100),
            }

            if i in anomaly_set:
                row = _apply_anomaly(row, anomaly_set[i])
                row["util_pct"]   = np.clip(row["util_pct"],   0, 100)
                row["iowait_pct"] = np.clip(row["iowait_pct"], 0, 100)
                row["svctm"]      = min(row["svctm"], row["r_await"])  # svctm can't exceed await

            rows.append(row)

    raw_df = pd.DataFrame(rows)
    raw_df = raw_df.sort_values(["device", "timestamp"]).reset_index(drop=True)

    curated_df = _derive_curated_fields(raw_df)

    return raw_df, curated_df


if __name__ == "__main__":
    raw_df, curated_df = generate_iostat_like_data()

    from pathlib import Path
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data/curated").mkdir(parents=True, exist_ok=True)
    raw_df.to_csv("data/raw/generated_iostat.csv", index=False)
    curated_df.to_csv("data/curated/generated_iostat_curated.csv", index=False)

    print(f"Raw dataset:     {len(raw_df)} rows, {raw_df['device'].nunique()} devices")
    print(f"Curated dataset: {len(curated_df)} rows, {len(curated_df.columns)} columns")
    print(f"Date range:      {raw_df['timestamp'].min()[:10]} → {raw_df['timestamp'].max()[:10]}")
    print(f"\nNew raw fields:     rrqm_s, wrqm_s, rareq_sz, wareq_sz, svctm, iowait_pct")
    print(f"New curated fields: throughput_mb_s, iops_total, merge_rate_total, merge_efficiency,")
    print(f"                    await_ratio, svctm_await_ratio, queue_efficiency,")
    print(f"                    write_amplification, iowait_pressure")