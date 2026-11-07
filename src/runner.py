"""Command-line runner for BCB SGS ingestion with local historical storage."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from uuid import uuid4

import yaml

from src.extract.bcb_sgs import extract_series
from src.storage import upsert_silver, write_bronze_snapshot
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
    last_n: int = 20,
) -> dict[str, int]:
    """Preserve Bronze snapshots and merge Silver with existing local files.

    bronze_rows counts this extraction; silver_rows counts accumulated rows.
    Remote durability and concurrent writers must be managed separately.
    """
    indicators = load_indicators(config_path)
    run_id = uuid4().hex
    bronze_dir = output_path / "bronze"
    silver_dir = output_path / "silver"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)
    bronze_rows = 0
    silver_rows = 0
    LOGGER.info("Starting pipeline run %s", run_id)

    for indicator in indicators:
        if indicator.get("source") != "bcb_sgs":
            LOGGER.warning("Skipping unsupported source: %s", indicator.get("source"))
            continue
        series_code = str(indicator["source_series_code"])
        LOGGER.info("Extracting BCB SGS series %s", series_code)
        extraction = extract_series(series_code=series_code, last_n=last_n)
        bronze = build_bronze_dataframe(extraction)
        snapshot_path = write_bronze_snapshot(bronze, output_path, series_code, run_id)
        LOGGER.info("Bronze snapshot saved: %s", snapshot_path)
        # Compatibility output: latest snapshot, not the historical dataset.
        bronze.to_parquet(bronze_dir / f"bcb_sgs_{series_code}.parquet", index=False)
        silver = build_silver_dataframe(
            raw_observations=extraction.payload,
            series_code=series_code,
            indicator_code=indicator["indicator_code"],
            indicator_name=indicator["name"],
            frequency=indicator["frequency"],
            unit=indicator["unit"],
        )
        silver_path = silver_dir / f"{indicator['indicator_code'].lower()}.parquet"
        accumulated = upsert_silver(silver, silver_path)
        bronze_rows += len(bronze)
        silver_rows += len(accumulated)
        LOGGER.info(
            "Series %s: %d extracted Bronze rows, %d incoming Silver rows, %d accumulated Silver rows",
            series_code, len(bronze), len(silver), len(accumulated),
        )

    LOGGER.info("Pipeline completed: %d Bronze rows, %d accumulated Silver rows", bronze_rows, silver_rows)
    return {"bronze_rows": bronze_rows, "silver_rows": silver_rows}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the BCB SGS ingestion pipeline.")
    parser.add_argument("--config", type=Path, default=Path("config/indicators.yml"),
                        help="Path to the indicators configuration file.")
    parser.add_argument("--output", type=Path, default=Path("data"),
                        help="Base directory for Bronze and Silver Parquet outputs.")
    parser.add_argument("--last-n", type=int, default=20,
                        help="Number of most recent observations to extract per series (1-20).")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run_pipeline(config_path=args.config, output_path=args.output, last_n=args.last_n)


if __name__ == "__main__":
    main()
