REQUIRED_COLUMNS = [
    "device",
    "timestamp",
    "r_s",
    "w_s",
    "rmb_s",
    "wmb_s",
    "r_await",
    "w_await",
    "aqu_sz",
    "util_pct",
]


def validate_schema(df):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]

    if missing:
        raise ValueError(f"Missing columns: {missing}")

    return True
