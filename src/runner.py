"""Command-line runner for the BCB SGS ingestion pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
import yaml

from src.extract.bcb_sgs import extract_series
from src.transform.bronze import build_bronze_dataframe
from src.transform.silver import build_silver_dataframe

LOGGER = logging.getLogger(__name__)


def load_indicators(config_path: Path) -> list[dict]:
    """Load and validate the indicator configuration file."""
    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    indicators = config.get("indicators", [])
    if not indicators:
        raise ValueError(f"No indicators found in {config_path}")

    return indicators


def run_pipeline(
    config_path: Path,
    output_path: Path,
    last_n: int = 100,
) -> dict[str, int]:
    """Extract BCB series and persist Bronze and Silver datasets.

    Args:
        config_path: Path to the indicators YAML file.
        output_path: Base directory for Parquet outputs.
        last_n: Number of most recent observations to extract per series.

    Returns:
        A dictionary with the number of rows written to each layer.
    """
    indicators = load_indicators(config_path)

    bronze_dir = output_path / "bronze"
    silver_dir = output_path / "silver"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)

    bronze_frames: list[pd.DataFrame] = []
    silver_frames: list[pd.DataFrame] = []

    for indicator in indicators:
        if indicator.get("source") != "bcb_sgs":
            LOGGER.warning("Skipping unsupported source: %s", indicator.get("source"))
            continue

        series_code = str(indicator["source_series_code"])
        LOGGER.info("Extracting BCB SGS series %s", series_code)

        extraction = extract_series(series_code=series_code, last_n=last_n)
        bronze = build_bronze_dataframe(extraction)
        silver = build_silver_dataframe(
            raw_observations=extraction.payload,
            series_code=series_code,
            indicator_code=indicator["indicator_code"],
            indicator_name=indicator["name"],
            frequency=indicator["frequency"],
            unit=indicator["unit"],
        )

        bronze_path = bronze_dir / f"bcb_sgs_{series_code}.parquet"
        silver_path = silver_dir / f"{indicator['indicator_code'].lower()}.parquet"

        bronze.to_parquet(bronze_path, index=False)
        silver.to_parquet(silver_path, index=False)

        bronze_frames.append(bronze)
        silver_frames.append(silver)

        LOGGER.info(
            "Series %s: %d Bronze rows and %d Silver rows saved",
            series_code,
            len(bronze),
            len(silver),
        )

    bronze_rows = sum(len(frame) for frame in bronze_frames)
    silver_rows = sum(len(frame) for frame in silver_frames)

    LOGGER.info("Pipeline completed: %d Bronze rows, %d Silver rows", bronze_rows, silver_rows)
    return {"bronze_rows": bronze_rows, "silver_rows": silver_rows}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the BCB SGS ingestion pipeline.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/indicators.yml"),
        help="Path to the indicators configuration file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data"),
        help="Base directory for Bronze and Silver Parquet outputs.",
    )
    parser.add_argument(
        "--last-n",
        type=int,
        default=100,
        help="Number of most recent observations to extract per series.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run_pipeline(config_path=args.config, output_path=args.output, last_n=args.last_n)


if __name__ == "__main__":
    main()
