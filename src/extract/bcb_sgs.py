"""Extractor for the Central Bank of Brazil SGS time-series API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"


@dataclass(frozen=True)
class ExtractionResult:
    series_code: str
    request_url: str
    http_status: int
    payload: list[dict[str, Any]]


def extract_series(
    series_code: str,
    start_date: str | None = None,
    end_date: str | None = None,
    last_n: int | None = None,
    timeout: int = 30,
) -> ExtractionResult:
    """Extract observations from a BCB SGS series.

    Args:
        series_code: BCB SGS series identifier.
        start_date: Start date in DD/MM/YYYY format.
        end_date: End date in DD/MM/YYYY format.
        last_n: Number of most recent observations to retrieve.
        timeout: HTTP request timeout in seconds.

    Returns:
        An ExtractionResult containing the raw payload and request metadata.
    """
    url = BASE_URL.format(series_code=series_code)
    params: dict[str, str | int] = {"formato": "json"}

    if last_n is not None:
        if last_n <= 0:
            raise ValueError("last_n must be greater than zero")
        url = f"{url}/ultimos/{last_n}"
    else:
        if start_date:
            params["dataInicial"] = start_date
        if end_date:
            params["dataFinal"] = end_date

    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(f"Unexpected payload type for series {series_code}: {type(payload)}")

    return ExtractionResult(
        series_code=series_code,
        request_url=response.url,
        http_status=response.status_code,
        payload=payload,
    )
