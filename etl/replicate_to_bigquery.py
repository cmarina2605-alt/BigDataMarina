"""
Supabase → Google BigQuery replication script.

Reads all 12 tables from Supabase (PostgreSQL) and loads them into
a BigQuery dataset, replacing existing data on each run.

Requirements:
  pip install google-cloud-bigquery pyarrow db-dtypes

Environment variables (add to .env):
  BQ_PROJECT_ID      GCP project ID
  BQ_DATASET_ID      BigQuery dataset name (e.g. bigdatamarina)
  BQ_CREDENTIALS     Path to GCP service account JSON key file
"""

import os
import pg8000.dbapi
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account


# ======================================================
# CONFIGURATION
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# Supabase connection
PG_CONN = dict(
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT", 6543)),
    database=os.getenv("DB_NAME"),
    ssl_context=True,
)

# BigQuery connection
BQ_PROJECT = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET_ID")
BQ_CREDENTIALS_PATH = os.getenv("BQ_CREDENTIALS")


# ======================================================
# BIGQUERY SCHEMAS
# Maps each Supabase table to an explicit BQ schema.
# Explicit schemas prevent type-inference surprises on
# nullable or mixed-type columns.
# ======================================================

BQ = bigquery.enums.SqlTypeNames

SCHEMAS = {
    "population_total": [
        bigquery.SchemaField("id",          BQ.INT64),
        bigquery.SchemaField("level",       BQ.STRING),
        bigquery.SchemaField("territory",   BQ.STRING),
        bigquery.SchemaField("year",        BQ.INT64),
        bigquery.SchemaField("population",  BQ.FLOAT64),
        bigquery.SchemaField("sex",         BQ.STRING),
        bigquery.SchemaField("age_group",   BQ.STRING),
        bigquery.SchemaField("nationality", BQ.STRING),
        bigquery.SchemaField("concept",     BQ.STRING),
        bigquery.SchemaField("data_type",   BQ.STRING),
        bigquery.SchemaField("unit",        BQ.STRING),
        bigquery.SchemaField("tipo_dato",   BQ.STRING),
        bigquery.SchemaField("periodo",     BQ.STRING),
        bigquery.SchemaField("created_at",  BQ.TIMESTAMP),
    ],
    "population_eustat_total": [
        bigquery.SchemaField("id",          BQ.INT64),
        bigquery.SchemaField("province",    BQ.STRING),
        bigquery.SchemaField("nationality", BQ.STRING),
        bigquery.SchemaField("year",        BQ.INT64),
        bigquery.SchemaField("population",  BQ.INT64),
    ],
    "population_eustat_nationality": [
        bigquery.SchemaField("id",                     BQ.INT64),
        bigquery.SchemaField("ccaa",                   BQ.STRING),
        bigquery.SchemaField("province",               BQ.STRING),
        bigquery.SchemaField("nationality",            BQ.STRING),
        bigquery.SchemaField("relation_with_activity", BQ.STRING),
        bigquery.SchemaField("sex",                    BQ.STRING),
        bigquery.SchemaField("year",                   BQ.INT64),
        bigquery.SchemaField("population",             BQ.FLOAT64),
        bigquery.SchemaField("created_at",             BQ.TIMESTAMP),
    ],
    "epa_contract_stats": [
        bigquery.SchemaField("id",         BQ.INT64),
        bigquery.SchemaField("sex",        BQ.STRING),
        bigquery.SchemaField("age_group",  BQ.STRING),
        bigquery.SchemaField("indicator",  BQ.STRING),
        bigquery.SchemaField("disability", BQ.STRING),
        bigquery.SchemaField("year",       BQ.INT64),
        bigquery.SchemaField("value",      BQ.FLOAT64),
    ],
    "migration_birth_country": [
        bigquery.SchemaField("id",          BQ.INT64),
        bigquery.SchemaField("region",      BQ.STRING),
        bigquery.SchemaField("indicator",   BQ.STRING),
        bigquery.SchemaField("sex",         BQ.STRING),
        bigquery.SchemaField("age_group",   BQ.STRING),
        bigquery.SchemaField("nationality", BQ.STRING),
        bigquery.SchemaField("periodicity", BQ.STRING),
        bigquery.SchemaField("data_type",   BQ.STRING),
        bigquery.SchemaField("year",        BQ.INT64),
        bigquery.SchemaField("value",       BQ.FLOAT64),
        bigquery.SchemaField("unit",        BQ.STRING),
    ],
    "crime_detentions": [
        bigquery.SchemaField("id",          BQ.INT64),
        bigquery.SchemaField("province",    BQ.STRING),
        bigquery.SchemaField("region",      BQ.STRING),
        bigquery.SchemaField("nationality", BQ.STRING),
        bigquery.SchemaField("sex",         BQ.STRING),
        bigquery.SchemaField("year",        BQ.INT64),
        bigquery.SchemaField("value",       BQ.INT64),
    ],
    "crime_detentions_total": [
        bigquery.SchemaField("id",         BQ.INT64),
        bigquery.SchemaField("territory",  BQ.STRING),
        bigquery.SchemaField("crime_type", BQ.STRING),
        bigquery.SchemaField("age_group",  BQ.STRING),
        bigquery.SchemaField("sex",        BQ.STRING),
        bigquery.SchemaField("year",       BQ.INT64),
        bigquery.SchemaField("value",      BQ.INT64),
    ],
    "ine_employment": [
        bigquery.SchemaField("id",                BQ.INT64),
        bigquery.SchemaField("relation_activity", BQ.STRING),
        bigquery.SchemaField("territory",         BQ.STRING),
        bigquery.SchemaField("sex",               BQ.STRING),
        bigquery.SchemaField("sector",            BQ.STRING),
        bigquery.SchemaField("nationality",       BQ.STRING),
        bigquery.SchemaField("year",              BQ.INT64),
        bigquery.SchemaField("value",             BQ.FLOAT64),
    ],
    "eustat_activity_nationality": [
        bigquery.SchemaField("id",          BQ.INT64),
        bigquery.SchemaField("rate_type",   BQ.STRING),
        bigquery.SchemaField("province",    BQ.STRING),
        bigquery.SchemaField("nationality", BQ.STRING),
        bigquery.SchemaField("quarter",     BQ.STRING),
        bigquery.SchemaField("year",        BQ.INT64),
        bigquery.SchemaField("value",       BQ.FLOAT64),
    ],
    "elections_parlamento_vasco": [
        bigquery.SchemaField("id",         BQ.INT64),
        bigquery.SchemaField("year",       BQ.STRING),
        bigquery.SchemaField("month",      BQ.STRING),
        bigquery.SchemaField("party_name", BQ.STRING),
        bigquery.SchemaField("seats",      BQ.INT64),
    ],
    "housing_prices_annual": [
        bigquery.SchemaField("id",           BQ.INT64),
        bigquery.SchemaField("year",         BQ.INT64),
        bigquery.SchemaField("province",     BQ.STRING),
        bigquery.SchemaField("region_type",  BQ.STRING),
        bigquery.SchemaField("price_per_m2", BQ.FLOAT64),
        bigquery.SchemaField("source",       BQ.STRING),
        bigquery.SchemaField("created_at",   BQ.TIMESTAMP),
    ],
    "ine_poverty_stats": [
        bigquery.SchemaField("id",        BQ.INT64),
        bigquery.SchemaField("level",     BQ.STRING),
        bigquery.SchemaField("territory", BQ.STRING),
        bigquery.SchemaField("indicator", BQ.STRING),
        bigquery.SchemaField("year",      BQ.INT64),
        bigquery.SchemaField("value",     BQ.FLOAT64),
    ],
}


# ======================================================
# HELPERS
# ======================================================

def get_pg_conn():
    return pg8000.dbapi.connect(**PG_CONN)


def get_bq_client():
    credentials = service_account.Credentials.from_service_account_file(
        BQ_CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=BQ_PROJECT, credentials=credentials)


def ensure_dataset(client):
    dataset_ref = f"{BQ_PROJECT}.{BQ_DATASET}"
    try:
        client.get_dataset(dataset_ref)
        print(f"  Dataset '{BQ_DATASET}' already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        client.create_dataset(dataset)
        print(f"  Dataset '{BQ_DATASET}' created (location=EU).")


def read_table(conn, table_name):
    print(f"  Reading '{table_name}' from Supabase...", end=" ", flush=True)
    df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
    print(f"{len(df):,} rows")
    return df


def load_to_bq(client, df, table_name, schema):
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"  Loading '{table_name}' into BigQuery...", end=" ", flush=True)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # wait for completion
    print(f"done ({job.output_rows:,} rows written)")


# ======================================================
# MAIN
# ======================================================

def main():
    # Validate required env vars
    missing = [
        var for var in ("BQ_PROJECT_ID", "BQ_DATASET_ID", "BQ_CREDENTIALS")
        if not os.getenv(var)
    ]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Add them to your .env file and retry."
        )

    print("=" * 55)
    print("Supabase → BigQuery replication")
    print("=" * 55)

    print("\n[1/3] Initialising BigQuery client...")
    bq_client = get_bq_client()
    ensure_dataset(bq_client)

    print("\n[2/3] Connecting to Supabase...")
    pg_conn = get_pg_conn()

    print("\n[3/3] Replicating tables...\n")
    errors = []

    for table_name, schema in SCHEMAS.items():
        try:
            df = read_table(pg_conn, table_name)
            load_to_bq(bq_client, df, table_name, schema)
        except Exception as exc:
            print(f"  ERROR on '{table_name}': {exc}")
            errors.append((table_name, exc))

    pg_conn.close()

    print("\n" + "=" * 55)
    if errors:
        print(f"Replication finished with {len(errors)} error(s):")
        for tbl, err in errors:
            print(f"  - {tbl}: {err}")
    else:
        print(f"All {len(SCHEMAS)} tables replicated successfully.")
    print("=" * 55)


if __name__ == "__main__":
    main()
