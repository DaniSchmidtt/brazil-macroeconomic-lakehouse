"""Bronze-layer transformations for BCB SGS extractions."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.extract.bcb_sgs import ExtractionResult


def build_bronze_dataframe(
    result: ExtractionResult,
    source_system: str = "bcb_sgs",
) -> pd.DataFrame:
    """Convert a raw BCB SGS extraction into a Bronze DataFrame."""
    extraction_ts = datetime.now(timezone.utc)

    return pd.DataFrame(
        {
            "source_system": [source_system] * len(result.payload),
            "source_series_code": [result.series_code] * len(result.payload),
            "extraction_ts": [extraction_ts] * len(result.payload),
            "raw_payload": [str(item) for item in result.payload],
            "http_status": [result.http_status] * len(result.payload),
            "request_url": [result.request_url] * len(result.payload),
        }
    )
