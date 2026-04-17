# Dashboard

Small serving-layer utility that renders the four dashboard charts
directly from the dbt marts.  It is the live consumer of the warehouse:
no intermediate CSV is kept on disk.

## What it does

For each of the four research questions the project answers, the
script runs a `SELECT` against the corresponding mart (or staging view)
on Supabase PostgreSQL and renders a matplotlib figure:

| Chart | Source model |
|---|---|
| `chart_foreign_share.png` | `stg_eustat_foreign_population_province` |
| `chart_crime_rates.png`   | `mart_immigration_crime_study_pv_rates` |
| `chart_poverty.png`       | `mart_immigration_poverty_pv` |
| `chart_housing.png`       | `mart_housing_foreign_share` |

## How to run

```bash
# from the project root, with the dbt virtualenv active
python dashboard/build_charts_from_marts.py
```

The script reads Supabase credentials from the `.env` file at the
project root and writes the PNGs to `dashboard/images/`.

## Why no CSV cache?

The previous `analysis_results/` folder shipped a frozen copy of every
dbt view as a CSV.  Since dbt is now the single source of truth for
every aggregate, caching them on disk only created a stale-data risk:
running `dbt run` would update the warehouse but not the CSV copies.
The dashboard now goes straight to the marts, which is the intended
consumption pattern.
