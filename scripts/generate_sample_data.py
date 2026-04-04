import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone


# ---------------------------------------------------------------------------
# Device profiles — realistic storage device archetypes
# ---------------------------------------------------------------------------
DEVICE_PROFILES = {
    "nvme0n1": {
        "type": "NVMe SSD (OS/boot)",
        "r_s": (300, 60), "w_s": (150, 30),
        "rmb_s": (25, 5), "wmb_s": (12, 3),
        "r_await": (0.3, 0.15), "w_await": (0.5, 0.2),
        "aqu_sz": (0.4, 0.15), "util_pct": (25, 8),
    },
    "nvme1n1": {
        "type": "NVMe SSD (database)",
        "r_s": (800, 150), "w_s": (600, 120),
        "rmb_s": (50, 10), "wmb_s": (40, 8),
        "r_await": (0.2, 0.1), "w_await": (0.4, 0.15),
        "aqu_sz": (1.5, 0.5), "util_pct": (55, 12),
    },
    "sda": {
        "type": "SATA SSD (app tier)",
        "r_s": (200, 50), "w_s": (180, 40),
        "rmb_s": (15, 4), "wmb_s": (12, 3),
        "r_await": (1.0, 0.4), "w_await": (1.5, 0.5),
        "aqu_sz": (1.0, 0.3), "util_pct": (45, 10),
    },
    "sdb": {
        "type": "HDD (bulk storage/logs)",
        "r_s": (80, 25), "w_s": (120, 35),
        "rmb_s": (8, 3), "wmb_s": (10, 3),
        "r_await": (5.0, 2.0), "w_await": (7.0, 2.5),
        "aqu_sz": (2.5, 0.8), "util_pct": (65, 12),
    },
    "dm-0": {
        "type": "LVM/Device mapper (RAID)",
        "r_s": (400, 80), "w_s": (350, 70),
        "rmb_s": (30, 6), "wmb_s": (25, 5),
        "r_await": (0.8, 0.3), "w_await": (1.2, 0.4),
        "aqu_sz": (1.8, 0.6), "util_pct": (50, 10),
    },
}

# ---------------------------------------------------------------------------
# Anomaly scenarios — realistic failure/stress patterns
# ---------------------------------------------------------------------------
ANOMALY_SCENARIOS = [
    {"name": "disk_saturation",  "fields": {"util_pct": (95, 3), "aqu_sz": (8, 2), "r_await": (15, 5), "w_await": (20, 6)}},
    {"name": "iops_burst",       "fields": {"r_s": (5.0, "mul"), "w_s": (4.0, "mul")}},
    {"name": "latency_spike",    "fields": {"r_await": (25, 8), "w_await": (35, 10), "aqu_sz": (6, 2)}},
    {"name": "write_storm",      "fields": {"w_s": (8.0, "mul"), "wmb_s": (6.0, "mul"), "w_await": (12, 4)}},
    {"name": "read_flood",       "fields": {"r_s": (6.0, "mul"), "rmb_s": (5.0, "mul"), "r_await": (10, 3)}},
    {"name": "queue_buildup",    "fields": {"aqu_sz": (12, 3), "util_pct": (90, 4), "r_await": (8, 3), "w_await": (10, 3)}},
]


def _time_of_day_factor(hour, day_of_week):
    """Business-hours load pattern: peaks 9-17 on weekdays, lower on weekends."""
    if day_of_week >= 5:  # weekend
        base = 0.4
    else:
        base = 1.0

    # Bell curve centered at 13:00
    hourly = 0.3 + 0.7 * np.exp(-0.5 * ((hour - 13) / 4) ** 2)
    return base * hourly


def _apply_anomaly(row, profile):
    """Replace fields with anomaly-level values."""
    scenario = ANOMALY_SCENARIOS[np.random.randint(len(ANOMALY_SCENARIOS))]
    for field, spec in scenario["fields"].items():
        if spec[1] == "mul":
            row[field] = abs(row[field]) * np.random.uniform(spec[0] * 0.8, spec[0] * 1.2)
        else:
            row[field] = max(np.random.normal(spec[0], spec[1]), 0)
    return row


def generate_iostat_like_data(days=7, interval_minutes=5):
    """
    Generate realistic storage telemetry data.

    Args:
        days: Number of days to simulate (default 7 -> ~2000 samples/device)
        interval_minutes: Sampling interval in minutes
    """
    samples_per_device = int((days * 24 * 60) / interval_minutes)
    rows = []

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    for device, profile in DEVICE_PROFILES.items():
        # Per-device drift: gradual degradation over time for some devices
        has_degradation = np.random.random() < 0.3
        degradation_rate = np.random.uniform(0.001, 0.003) if has_degradation else 0

        # Anomaly windows: schedule 2-5 anomaly bursts per device
        num_anomaly_windows = np.random.randint(2, 6)
        anomaly_starts = sorted(np.random.randint(0, samples_per_device, num_anomaly_windows))
        anomaly_durations = np.random.randint(3, 15, num_anomaly_windows)
        anomaly_set = set()
        for a_start, a_dur in zip(anomaly_starts, anomaly_durations):
            for j in range(a_dur):
                anomaly_set.add(a_start + j)

        for i in range(samples_per_device):
            jitter = timedelta(seconds=np.random.uniform(-30, 30))
            ts = start + timedelta(minutes=i * interval_minutes) + jitter

            hour = ts.hour
            day_of_week = ts.weekday()
            load = _time_of_day_factor(hour, day_of_week)

            # Gradual degradation over time
            drift = 1.0 + degradation_rate * i

            row = {
                "device": device,
                "timestamp": ts.isoformat(),
                "r_s":     max(np.random.normal(profile["r_s"][0], profile["r_s"][1]) * load * drift, 0),
                "w_s":     max(np.random.normal(profile["w_s"][0], profile["w_s"][1]) * load * drift, 0),
                "rmb_s":   max(np.random.normal(profile["rmb_s"][0], profile["rmb_s"][1]) * load, 0),
                "wmb_s":   max(np.random.normal(profile["wmb_s"][0], profile["wmb_s"][1]) * load, 0),
                "r_await": max(np.random.lognormal(np.log(profile["r_await"][0] * drift), profile["r_await"][1]), 0.01),
                "w_await": max(np.random.lognormal(np.log(profile["w_await"][0] * drift), profile["w_await"][1]), 0.01),
                "aqu_sz":  max(np.random.lognormal(np.log(profile["aqu_sz"][0]), profile["aqu_sz"][1]) * load, 0),
                "util_pct": np.clip(np.random.normal(profile["util_pct"][0] * load * drift, profile["util_pct"][1]), 0, 100),
            }

            # Apply anomaly if in anomaly window
            if i in anomaly_set:
                row = _apply_anomaly(row, profile)
                row["util_pct"] = np.clip(row["util_pct"], 0, 100)

            rows.append(row)

    df = pd.DataFrame(rows)
    df = df.sort_values(["device", "timestamp"]).reset_index(drop=True)
    return df


if __name__ == "__main__":
    df = generate_iostat_like_data()
    df.to_csv("data/raw/generated_iostat.csv", index=False)
    print(f"Synthetic dataset generated: {len(df)} rows, {df['device'].nunique()} devices, "
          f"{df['timestamp'].min()[:10]} to {df['timestamp'].max()[:10]}")
