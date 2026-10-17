"""Silver-layer transformations for BCB SGS observations."""

from __future__ import annotations

import pandas as pd

REQUIRED_COLUMNS = ["data", "valor"]


def build_silver_dataframe(
    raw_observations: list[dict[str, str]],
    series_code: str,
    indicator_code: str,
    indicator_name: str,
    frequency: str,
    unit: str,
) -> pd.DataFrame:
    """Normalize raw BCB SGS observations into the Silver model."""
    if not raw_observations:
        return pd.DataFrame(columns=REQUIRED_COLUMNS + ["source_series_code", "indicator_code"])

    df = pd.DataFrame(raw_observations)

    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["observation_date"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
    df["value"] = pd.to_numeric(df["valor"], errors="coerce")

    invalid = df["observation_date"].isna() | df["value"].isna()
    if invalid.any():
        raise ValueError(f"Found {int(invalid.sum())} invalid observations in series {series_code}")

    df["source_series_code"] = series_code
    df["indicator_code"] = indicator_code
    df["indicator_name"] = indicator_name
    df["frequency"] = frequency
    df["unit"] = unit

    df = df.drop_duplicates(
        subset=["source_series_code", "observation_date"],
        keep="last",
    )

    return df[
        [
            "source_series_code",
            "indicator_code",
            "indicator_name",
            "frequency",
            "unit",
            "observation_date",
            "value",
        ]
    ].reset_index(drop=True)
