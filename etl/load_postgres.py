"""
PostgreSQL data loading script.

This script loads cleaned CSV files into PostgreSQL tables.
It covers multiple data domains, including demography, employment,
migration, crime, housing, elections, and social indicators.

The loading process uses the PostgreSQL COPY command for efficiency
and assumes that all CSV files have already passed pre-ingestion
data quality checks.
"""

import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
import time


# ======================================================
# PATH CONFIGURATION
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_clean"

load_dotenv(BASE_DIR / ".env")


# ======================================================
# DATABASE CONNECTION
# ======================================================

def get_conn():
    """
    Creates and returns a PostgreSQL database connection
    using environment variables.
    """
    return psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        dbname=os.getenv("DB_NAME"),
        sslmode="require"
    )


# ======================================================
# GLOBAL TRUNCATION
# ======================================================

def truncate_all():
    """
    Empties all target tables before loading new data.

    This operation ensures idempotency of the loading process:
    running the pipeline multiple times produces the same
    database state.
    """

    print("\nTruncating tables...")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        TRUNCATE TABLE
            population_total,
            population_eustat_total,
            population_eustat_nationality,
            epa_contract_stats,
            migration_birth_country,
            crime_detentions,
            crime_detentions_total,
            ine_employment,
            eustat_activity_nationality,
            elections_parlamento_vasco,
            housing_prices_annual,
            ine_poverty_stats,
            ine_padron_foreign,
            ine_ecp_foreign
        RESTART IDENTITY CASCADE;
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("Tables truncated successfully.")


# ======================================================
# COPY HELPER
# ======================================================

def copy_csv(table, filepath, columns=None):
    """
    Loads a CSV file into a PostgreSQL table using COPY.

    Parameters:
    - table: Target table name
    - filepath: Path to the CSV file
    - columns: Optional list of column names to define the load order
    """

    print(f"Loading table: {table}")

    if not filepath.exists():
        print("File not found:", filepath)
        return

    conn = get_conn()
    cur = conn.cursor()

    try:
        cols = f"({','.join(columns)})" if columns else ""

        sql = f"""
            COPY {table} {cols}
            FROM STDIN
            WITH CSV HEADER
        """

        print("COPY command:", sql)

        with open(filepath, "r", encoding="utf-8") as f:
            cur.copy_expert(sql, f)

        conn.commit()
        print("Load completed:", table)

    except Exception as e:
        conn.rollback()
        print("Error loading table:", table)
        print("Error message:", e)

    finally:
        cur.close()
        conn.close()


# ======================================================
# DATA LOADERS
# ======================================================

def load_population_total():
    copy_csv(
        "population_total",
        DATA_DIR / "population_territory_part1.csv",
        columns=[
            "level",
            "territory",
            "year",
            "population",
            "sex",
            "age_group",
            "nationality",
            "concept",
            "data_type",
            "unit",
            "tipo_dato",
            "periodo"
        ]
    )
    copy_csv(
        "population_total",
        DATA_DIR / "population_territory_part2.csv",
        columns=[
            "level",
            "territory",
            "year",
            "population",
            "sex",
            "age_group",
            "nationality",
            "concept",
            "data_type",
            "unit",
            "tipo_dato",
            "periodo"
        ]
    )
    copy_csv(
        "population_total",
        DATA_DIR / "population_territory_part3.csv",
        columns=[
            "level",
            "territory",
            "year",
            "population",
            "sex",
            "age_group",
            "nationality",
            "concept",
            "data_type",
            "unit",
            "tipo_dato",
            "periodo"
        ]
    )

def load_eustat_total():
    """
    IMPORTANT:
    This table does NOT have an auto-increment id.
    Columns must be explicitly provided to avoid COPY errors.
    """
    copy_csv(
        "population_eustat_total",
        DATA_DIR / "eustat_population_clean.csv",
        columns=[
            "province",
            "nationality",
            "year",
            "population"
        ]
    )


def load_eustat_nationality():
    copy_csv(
        "population_eustat_nationality",
        DATA_DIR / "eustat_population_nationality_clean.csv",
        columns=[
            "ccaa",
            "province",
            "nationality",
            "relation_with_activity",
            "sex",
            "year",
            "population"
        ]
    )


def load_epa_contract():
    """
    IMPORTANT:
    This table uses a composite primary key and has no id column.
    COPY must specify all columns explicitly.
    """
    copy_csv(
        "epa_contract_stats",
        DATA_DIR / "epa_contract_clean.csv",
        columns=[
            "sex",
            "age_group",
            "indicator",
            "disability",
            "year",
            "value"
        ]
    )


def load_birth_country():
    copy_csv(
        "migration_birth_country(region, indicator, sex, age_group, nationality, periodicity, data_type, year, value, unit)",
        DATA_DIR / "birth_country_clean.csv"
    )


def load_crime():
    copy_csv(
        "crime_detentions",
        DATA_DIR / "crime_detentions_clean.csv",
        columns=[
            "province",
            "region",
            "nationality",
            "sex",
            "year",
            "value"
        ]
    )


def load_crime_total():
    copy_csv(
        "crime_detentions_total",
        DATA_DIR / "crime_total_clean.csv",
        columns=[
            "territory",
            "crime_type",
            "age_group",
            "sex",
            "year",
            "value"
        ]
    )


def load_ine_employ():
    """
    IMPORTANT:
    This table also has no id column.
    Explicit column mapping is required.
    """
    copy_csv(
        "ine_employment",
        DATA_DIR / "ine_employment_clean.csv",
        columns=[
            "relation_activity",
            "territory",
            "sex",
            "sector",
            "nationality",
            "year",
            "value"
        ]
    )


def load_eustat_activity():
    copy_csv(
        "eustat_activity_nationality",
        DATA_DIR / "eustat_activity_nationality_clean.csv",
        columns=[
            "rate_type",
            "province",
            "nationality",
            "quarter",
            "year",
            "value"
        ]
    )


def load_elections():
    copy_csv(
        "elections_parlamento_vasco",
        DATA_DIR / "elections_clean.csv",
        columns=[
            "party_name",
            "year",
            "month",
            "seats"
        ]
    )


def load_housing_prices():
    copy_csv(
        "housing_prices_annual",
        DATA_DIR / "housing_prices_annual.csv",
        columns=[
            "year",
            "province",
            "region_type",
            "source",
            "price_per_m2"
        ]
    )


def load_ine_poverty():
    copy_csv(
        "ine_poverty_stats",
        DATA_DIR / "ine_pobreza.csv",
        columns=[
            "level",
            "territory",
            "year",
            "indicator",
            "value"
        ]
    )


def load_ine_padron_foreign():
    copy_csv(
        "ine_padron_foreign",
        DATA_DIR / "ine_foreign_population_province.csv",
        columns=[
            "province",
            "province_std",
            "year",
            "foreign_population"
        ]
    )


def load_ine_ecp_foreign():
    copy_csv(
        "ine_ecp_foreign",
        DATA_DIR / "ine_ecp_foreign_population_province.csv",
        columns=[
            "province_std",
            "year",
            "foreign_population",
            "total_population",
            "foreign_population_pct"
        ]
    )


# ======================================================
# MAIN EXECUTION
# ======================================================

if __name__ == "__main__":

    print("\n========== LOAD POSTGRES ==========")
    start = time.time()

    truncate_all()

    load_population_total()
    load_eustat_total()
    load_eustat_nationality()
    load_epa_contract()
    load_birth_country()
    load_crime()
    load_crime_total()
    load_ine_employ()
    load_eustat_activity()
    load_elections()
    load_housing_prices()
    load_ine_poverty()
    load_ine_padron_foreign()
    load_ine_ecp_foreign()

    print(f"\nData loading completed in {round(time.time() - start, 2)} seconds")
