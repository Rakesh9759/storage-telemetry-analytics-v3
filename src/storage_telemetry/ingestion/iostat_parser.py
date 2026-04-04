import re
import pandas as pd
from datetime import datetime


def parse_iostat_file(file_path: str) -> pd.DataFrame:
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path)

    rows = []
    current_timestamp = datetime.utcnow()

    with open(file_path, "r") as f:
        lines = f.readlines()

    header_idx = None

    for i, line in enumerate(lines):
        if "Device" in line and "r/s" in line:
            header_idx = i
            continue

        if header_idx is not None and line.strip():
            parts = re.split(r"\s+", line.strip())

            if len(parts) < 9:
                continue

            row = {
                "device": parts[0],
                "timestamp": current_timestamp.isoformat(),
                "r_s": float(parts[1]),
                "w_s": float(parts[2]),
                "rmb_s": float(parts[3]),
                "wmb_s": float(parts[4]),
                "r_await": float(parts[5]),
                "w_await": float(parts[6]),
                "aqu_sz": float(parts[7]),
                "util_pct": float(parts[8]),
            }

            rows.append(row)

    return pd.DataFrame(rows)
