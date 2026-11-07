"""Local historical Parquet persistence; remote durability is configured separately."""

from pathlib import Path
from uuid import uuid4

import pandas as pd

KEYS = ["source_series_code", "observation_date"]


def write_bronze_snapshot(df: pd.DataFrame, root: Path, series_code: str, run_id: str) -> Path:
    """Write a new snapshot without overwriting an existing execution."""
    if not all(value and all(c.isalnum() or c in "_-" for c in value)
               for value in (series_code, run_id)):
        raise ValueError("series_code and run_id must contain only letters, digits, _ or -")
    path = Path(root) / "bronze" / f"series={series_code}" / f"run={run_id}" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        try:
            df.to_parquet(stream, index=False)
        except Exception:
            path.unlink(missing_ok=True)
            raise
    return path


def upsert_silver(incoming: pd.DataFrame, path: Path) -> pd.DataFrame:
    """Merge by series and date; incoming values replace existing observations.

    Callers must serialize writes to the same destination.
    """
    path = Path(path)
    if incoming.empty:
        return pd.read_parquet(path) if path.exists() else incoming.copy()
    if not set(KEYS).issubset(incoming.columns):
        raise ValueError(f"Missing Silver keys: {KEYS}")
    if incoming[KEYS].isna().any().any():
        raise ValueError("Silver keys cannot be null")
    frames = [incoming]
    if path.exists():
        existing = pd.read_parquet(path)
        if set(existing.columns) != set(incoming.columns):
            raise ValueError("Silver schema mismatch")
        frames.insert(0, existing)
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(KEYS, keep="last").sort_values(KEYS).reset_index(drop=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        merged.to_parquet(temporary, index=False)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return merged
