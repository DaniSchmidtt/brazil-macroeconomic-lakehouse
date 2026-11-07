"""Historical persistence tests using temporary directories."""

import pandas as pd
import pytest

from src.storage import upsert_silver, write_bronze_snapshot


def observations(dates, values):
    return pd.DataFrame({
        "source_series_code": ["11"] * len(dates),
        "observation_date": pd.to_datetime(dates),
        "value": values,
    })


def test_bronze_preserves_each_execution(tmp_path):
    first = pd.DataFrame({"raw_payload": ["original"]})
    second = pd.DataFrame({"raw_payload": ["revised"]})
    a = write_bronze_snapshot(first, tmp_path, "11", "run_1")
    b = write_bronze_snapshot(second, tmp_path, "11", "run_2")
    pd.testing.assert_frame_equal(pd.read_parquet(a), first)
    pd.testing.assert_frame_equal(pd.read_parquet(b), second)
    with pytest.raises(FileExistsError):
        write_bronze_snapshot(second, tmp_path, "11", "run_1")
    pd.testing.assert_frame_equal(pd.read_parquet(a), first)


def test_bronze_rejects_unsafe_path(tmp_path):
    with pytest.raises(ValueError):
        write_bronze_snapshot(pd.DataFrame(), tmp_path, "../11", "run_1")


def test_silver_retains_history_and_applies_revisions(tmp_path):
    path = tmp_path / "silver" / "selic.parquet"
    first = observations(["2026-01-02", "2026-01-05"], [1.0, 2.0])
    upsert_silver(first, path)
    incoming = observations(["2026-01-05", "2026-01-06"], [2.5, 3.0])
    result = upsert_silver(incoming, path)
    expected = observations(["2026-01-02", "2026-01-05", "2026-01-06"], [1.0, 2.5, 3.0])
    pd.testing.assert_frame_equal(result, expected)
    pd.testing.assert_frame_equal(pd.read_parquet(path), expected)
    pd.testing.assert_frame_equal(upsert_silver(incoming, path), expected)


def test_empty_silver_update_preserves_existing_data(tmp_path):
    path = tmp_path / "silver.parquet"
    first = observations(["2026-01-02"], [1.0])
    upsert_silver(first, path)
    pd.testing.assert_frame_equal(upsert_silver(first.iloc[:0], path), first)


def test_invalid_update_does_not_change_silver(tmp_path):
    path = tmp_path / "silver.parquet"
    first = observations(["2026-01-02"], [1.0])
    upsert_silver(first, path)
    with pytest.raises(ValueError, match="Missing Silver keys"):
        upsert_silver(pd.DataFrame({"value": [2.0]}), path)
    with pytest.raises(ValueError, match="cannot be null"):
        upsert_silver(observations([None], [2.0]), path)
    with pytest.raises(ValueError, match="schema mismatch"):
        upsert_silver(first.assign(extra="unexpected"), path)
    pd.testing.assert_frame_equal(pd.read_parquet(path), first)
