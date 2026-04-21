"""
Post-dbt sanity check: verify that the 5 marts consumed by the Streamlit
dashboard are queryable on BigQuery and return at least one row each.

Runs at the end of `etl/run_pipeline.py` so a broken mart fails the pipeline
with a clear message, instead of the user discovering it when they open the
dashboard.
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

BQ_PROJECT = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET_ID")
BQ_CREDENTIALS_PATH = os.getenv("BQ_CREDENTIALS")

MARTS = [
    "mart_immigration_crime_study_pv_rates",
    "mart_labour_market_nationality_pv",
    "mart_housing_foreign_share",
    "mart_immigration_poverty_pv",
    "mart_elections_vs_demographics_pv",
]


def main() -> int:
    missing = [v for v in ("BQ_PROJECT_ID", "BQ_DATASET_ID", "BQ_CREDENTIALS")
               if not os.getenv(v)]
    if missing:
        print(f"ERROR: missing env vars: {', '.join(missing)}")
        return 1

    credentials = service_account.Credentials.from_service_account_file(
        BQ_CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(project=BQ_PROJECT, credentials=credentials)

    errors = []
    for mart in MARTS:
        table = f"`{BQ_PROJECT}.{BQ_DATASET}.{mart}`"
        try:
            row = next(iter(client.query(f"SELECT COUNT(*) AS n FROM {table}").result()))
            count = row["n"]
            if count == 0:
                errors.append(f"{mart}: 0 rows")
                print(f"  FAIL {mart}: 0 rows")
            else:
                print(f"  OK   {mart}: {count:,} rows")
        except Exception as exc:
            errors.append(f"{mart}: {exc}")
            print(f"  FAIL {mart}: {exc}")

    if errors:
        print(f"\n{len(errors)} mart(s) failed the smoke test.")
        return 1

    print("\nAll marts are queryable. Launch the dashboard with:")
    print("    streamlit run dashboard/app.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
