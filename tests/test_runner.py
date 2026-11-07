"""Tests for pipeline orchestration without external API calls."""

from unittest.mock import Mock

import pandas as pd
import pytest
import yaml

from src import runner
from src.extract import bcb_sgs
from src.extract.bcb_sgs import ExtractionResult


@pytest.fixture
def config_path(tmp_path):
    indicators = [
        {
            "indicator_code": "TEST_DAILY",
            "name": "Test daily indicator",
            "source": "bcb_sgs",
            "source_series_code": "11",
            "frequency": "daily",
            "unit": "test unit",
        },
        {
            "indicator_code": "TEST_MONTHLY",
            "name": "Test monthly indicator",
            "source": "bcb_sgs",
            "source_series_code": "20542",
            "frequency": "monthly",
            "unit": "test unit",
        },
    ]
    path = tmp_path / "indicators.yml"
    path.write_text(yaml.safe_dump({"indicators": indicators}), encoding="utf-8")
    return path


def test_load_indicators(config_path):
    indicators = runner.load_indicators(config_path)
    assert len(indicators) == 2
    assert indicators[0]["source_series_code"] == "11"


@pytest.mark.parametrize("content", ["", "{}", "indicators: []"])
def test_load_indicators_rejects_empty_configuration(tmp_path, content):
    path = tmp_path / "empty.yml"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError, match="No indicators found"):
        runner.load_indicators(path)


def test_load_indicators_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        runner.load_indicators(tmp_path / "missing.yml")


def test_pipeline_writes_readable_parquet(config_path, tmp_path, monkeypatch):
    payload = [
        {"data": "02/01/2026", "valor": "1.25"},
        {"data": "05/01/2026", "valor": "2.50"},
        {"data": "05/01/2026", "valor": "3.75"},
    ]

    def fake_extract(series_code, last_n):
        return ExtractionResult(
            series_code=series_code,
            request_url=f"https://example.test/{series_code}/ultimos/{last_n}",
            http_status=200,
            payload=payload,
        )

    extractor = Mock(side_effect=fake_extract)
    monkeypatch.setattr(runner, "extract_series", extractor)
    output = tmp_path / "output"
    counts = runner.run_pipeline(config_path, output)

    assert counts == {"bronze_rows": 6, "silver_rows": 4}
    assert extractor.call_count == 2
    extractor.assert_any_call(series_code="11", last_n=20)
    extractor.assert_any_call(series_code="20542", last_n=20)
    assert len(list((output / "bronze").glob("*.parquet"))) == 2
    assert len(list((output / "silver").glob("*.parquet"))) == 2

    for series_code, indicator_code in [("11", "TEST_DAILY"), ("20542", "TEST_MONTHLY")]:
        bronze = pd.read_parquet(output / "bronze" / f"bcb_sgs_{series_code}.parquet")
        silver = pd.read_parquet(output / "silver" / f"{indicator_code.lower()}.parquet")
        assert len(bronze) == 3
        assert len(silver) == 2
        assert set(bronze["source_series_code"]) == {series_code}
        assert set(silver["indicator_code"]) == {indicator_code}
        assert not silver.duplicated(["source_series_code", "observation_date"]).any()
        assert pd.api.types.is_datetime64_any_dtype(silver["observation_date"])
        assert pd.api.types.is_numeric_dtype(silver["value"])
        assert silver.loc[silver["observation_date"] == pd.Timestamp("2026-01-05"), "value"].item() == 3.75


def test_pipeline_propagates_extraction_error(config_path, tmp_path, monkeypatch):
    extractor = Mock(side_effect=RuntimeError("simulated extraction failure"))
    monkeypatch.setattr(runner, "extract_series", extractor)
    with pytest.raises(RuntimeError, match="simulated extraction failure"):
        runner.run_pipeline(config_path, tmp_path / "output")


@pytest.mark.parametrize("last_n", [-1, 0, 21, 100])
def test_extractor_rejects_invalid_limits_without_http(last_n, monkeypatch):
    request = Mock(side_effect=AssertionError("HTTP must not be called"))
    monkeypatch.setattr(bcb_sgs.requests, "get", request)
    with pytest.raises(ValueError, match="last_n must be between 1 and 20"):
        bcb_sgs.extract_series("11", last_n=last_n)
    request.assert_not_called()


@pytest.mark.parametrize("last_n", [1, 20])
def test_extractor_uses_latest_observations_endpoint(last_n, monkeypatch):
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados/ultimos/{last_n}"
    response = Mock()
    response.status_code = 200
    response.url = f"{url}?formato=json"
    response.json.return_value = [{"data": "02/01/2026", "valor": "1.25"}]
    request = Mock(return_value=response)
    monkeypatch.setattr(bcb_sgs.requests, "get", request)

    result = bcb_sgs.extract_series("11", last_n=last_n)

    request.assert_called_once_with(url, params={"formato": "json"}, timeout=30)
    response.raise_for_status.assert_called_once_with()
    assert result.payload == response.json.return_value
