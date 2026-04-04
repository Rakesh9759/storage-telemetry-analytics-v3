import pandas as pd


def add_rolling_baselines(
    df: pd.DataFrame,
    metric_cols: list[str],
    window: int = 5
) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_values(["device", "timestamp"]).reset_index(drop=True)

    for metric in metric_cols:
        out[f"{metric}_rolling_mean"] = (
            out.groupby("device")[metric]
            .transform(lambda s: s.rolling(window=window, min_periods=2).mean())
        )

        out[f"{metric}_rolling_std"] = (
            out.groupby("device")[metric]
            .transform(lambda s: s.rolling(window=window, min_periods=2).std())
        )

    return out
