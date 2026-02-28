"""
Post-ingestion data quality checks.

This script validates the state of the PostgreSQL database after data ingestion.
The goal is to ensure that the ingestion process has not introduced structural
issues and that the stored data remains analytically consistent.

The checks are aligned with the physical schema defined in init_db.py and are
designed to be resilient to partial loads and incremental pipeline execution.
"""

import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

# ======================================================
# DATABASE CONNECTION
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

def get_connection():
    """
    Creates a PostgreSQL connection using environment variables.
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
# TABLE CONFIGURATION (ALIGNED WITH init_db.py)
# ======================================================

TABLE_CONFIG = {

    "population_total": {
        "pk": ["level", "territory", "year"],
        "positive_cols": ["population"],
        "year_col": "year"
    },

    "population_eustat_total": {
        "pk": ["province", "year", "nationality"],
        "positive_cols": ["population"],
        "year_col": "year"
    },

    "population_eustat_nationality": {
        "pk": ["province", "nationality", "relation_with_activity", "sex", "year"],
        "positive_cols": ["population"],
        "year_col": "year"
    },

    "epa_contract_stats": {
        # Negative values are allowed for some EPA indicators
        "pk": ["sex", "age_group", "indicator", "disability", "year"],
        "positive_cols": [],
        "year_col": "year"
    },

    "migration_birth_country": {
        # age_group and sex may be NULL for aggregated series ("All ages")
        "pk": ["region", "indicator", "sex", "age_group", "nationality", "year"],
        "positive_cols": ["value"],
        "year_col": "year"
    },

    "crime_detentions": {
        "pk": ["province", "region", "nationality", "sex", "year"],
        "positive_cols": ["value"],
        "year_col": "year"
    },

    "crime_detentions_total": {
        "pk": ["territory", "crime_type", "age_group", "sex", "year"],
        "positive_cols": ["value"],
        "year_col": "year"
    },

    "ine_employment": {
        "pk": ["relation_activity", "territory", "sex", "sector", "nationality", "year"],
        "positive_cols": [],
        "year_col": "year"
    },

    "eustat_activity_nationality": {
        "pk": ["rate_type", "province", "nationality", "quarter", "year"],
        "positive_cols": [],
        "year_col": "year"
    },

    "housing_prices_annual": {
        "pk": ["year", "province", "source"],
        "positive_cols": ["price_per_m2"],
        "year_col": "year"
    },

    "ine_poverty_stats": {
        "pk": ["territory", "indicator", "year"],
        "positive_cols": ["value"],
        "year_col": "year"
    },

    "elections_parlamento_vasco": {
        "pk": ["year", "month", "party_name"],
        "positive_cols": ["seats"],
        "year_col": None
    }
}

# ======================================================
# HELPER FUNCTIONS
# ======================================================

def table_exists(cursor, table_name):
    """
    Checks whether a table exists in the public schema.
    """
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
        )
    """, (table_name,))
    return cursor.fetchone()[0]

# ======================================================
# VALIDATION LOGIC
# ======================================================

def validate_table(cursor, table_name, cfg):
    """
    Executes post-ingestion validation checks for a single table.
    All checks are informative and do not stop execution.
    """

    print(f"\nValidating table: {table_name}")

    # Table existence check
    if not table_exists(cursor, table_name):
        print(f"  WARNING: Table '{table_name}' does not exist (pipeline stage dependent)")
        return

    # Row count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_rows = cursor.fetchone()[0]
    print(f"  Total rows: {total_rows}")

    # Empty table check
    # An empty table is not considered a data quality error.
    if total_rows == 0:
        print("  WARNING: Table exists but contains no data")
        return

    # NULLs in logical primary key
    for col in cfg["pk"]:
        cursor.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL"
        )
        nulls = cursor.fetchone()[0]
        if nulls > 0:
            print(
                f"  WARNING: NULL values in PK column '{col}': {nulls}"
            )

    # Duplicated logical primary keys
    pk_cols = ", ".join(cfg["pk"])
    cursor.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT {pk_cols}, COUNT(*)
            FROM {table_name}
            GROUP BY {pk_cols}
            HAVING COUNT(*) > 1
        ) sub
    """)
    duplicates = cursor.fetchone()[0]
    if duplicates > 0:
        print(
            f"  WARNING: Duplicated logical PK rows: {duplicates}"
        )

    # Negative values check
    for col in cfg["positive_cols"]:
        cursor.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {col} < 0"
        )
        negatives = cursor.fetchone()[0]
        if negatives > 0:
            print(
                f"  WARNING: Negative values in '{col}': {negatives}"
            )

    # Year plausibility check
    year_col = cfg["year_col"]
    if year_col is not None:
        current_year = datetime.now().year
        cursor.execute(f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE {year_col} < 1990
               OR {year_col} > {current_year + 1}
        """)
        invalid_years = cursor.fetchone()[0]
        if invalid_years > 0:
            print(
                f"  WARNING: Years outside reasonable range: {invalid_years}"
            )

# ======================================================
# MAIN EXECUTION
# ======================================================

def run():
    """
    Runs post-ingestion data quality checks for all configured tables.
    """

    print("\n========== POST-INGESTION DATA QUALITY CHECKS ==========")

    conn = get_connection()
    cursor = conn.cursor()

    for table_name, cfg in TABLE_CONFIG.items():
        validate_table(cursor, table_name, cfg)

    cursor.close()
    conn.close()

    print("\nPost-ingestion checks completed successfully.")

if __name__ == "__main__":
    run()
