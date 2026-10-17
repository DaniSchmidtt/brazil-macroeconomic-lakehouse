# Brazil Macroeconomic Lakehouse

End-to-end data pipeline that consolidates Brazilian macroeconomic indicators from public APIs provided by IBGE, the Central Bank of Brazil (BCB), and Ipea.

## Problem

Brazilian economic indicators are distributed across multiple sources, formats, frequencies, and revision policies. This project builds a reliable data product for querying, analyzing, and visualizing macroeconomic time series.

## Architecture

```text
Public APIs
   ├── IBGE SIDRA
   ├── Central Bank of Brazil (BCB SGS)
   └── Ipeadata
          |
          v
Bronze - raw API payloads and extraction metadata
          |
          v
Silver - standardized, typed, deduplicated observations
          |
          v
Gold - analytical time-series model
          |
          v
Dashboards, SQL queries, and data-quality alerts
```

## MVP Scope

The first release ingests the following BCB SGS series:

| Indicator | Source | Series code |
|---|---|---:|
| Selic daily rate | BCB SGS | 11 |
| USD/BRL exchange rate | BCB SGS | 10813 |
| Total credit portfolio balance | BCB SGS | 20542 |

## How to run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make test
```

## Roadmap

- [x] Repository and project structure
- [ ] BCB SGS extractor
- [ ] Bronze layer
- [ ] Silver layer
- [ ] Data-quality checks
- [ ] Gold layer
- [ ] IBGE SIDRA ingestion
- [ ] Ipeadata ingestion
- [ ] Dashboard
- [ ] CI/CD and security checks
