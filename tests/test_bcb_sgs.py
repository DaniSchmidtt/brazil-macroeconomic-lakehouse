"""Unit tests for the BCB SGS extractor and Silver transformations."""

import pandas as pd
import pytest

from src.extract.bcb_sgs import ExtractionResult
from src.transform.bronze import build_bronze_dataframe
from src.transform.silver import build_silver_dataframe


def test_bronze_dataframe_contains_raw_payload():
    result = ExtractionResult(
        series_code="11",
        request_url="https://api.bcb.gov.br/test",
        http_status=200,
        payload=[{"data": "01/01/2026", "valor": "10.50"}],
    )

    df = build_bronze_dataframe(result)

    assert len(df) == 1
    assert df.loc[0, "source_series_code"] == "11"
    assert "01/01/2026" in df.loc[0, "raw_payload"]


def test_silver_dataframe_normalizes_observations():
    observations = [
        {"data": "01/01/2026", "valor": "10.50"},
        {"data": "02/01/2026", "valor": "10.60"},
    ]

    df = build_silver_dataframe(
        raw_observations=observations,
        series_code="11",
        indicator_code="SELIC_DAILY",
        indicator_name="Selic daily rate",
        frequency="daily",
        unit="% per year",
    )

    assert len(df) == 2
    assert df["value"].tolist() == [10.50, 10.60]
    assert df["observation_date"].tolist() == [
        pd.Timestamp("2026-01-01"),
        pd.Timestamp("2026-01-02"),
    ]


def test_silver_dataframe_rejects_invalid_date():
    observations = [{"data": "invalid-date", "valor": "10.50"}]

    with pytest.raises(ValueError):
        build_silver_dataframe(
            raw_observations=observations,
            series_code="11",
            indicator_code="SELIC_DAILY",
            indicator_name="Selic daily rate",
            frequency="daily",
            unit="% per year",
        )
