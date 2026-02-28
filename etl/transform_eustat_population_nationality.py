"""
Transform EUSTAT population by nationality (wide CSV to long CSV).

This script converts the EUSTAT population by nationality dataset
from a wide format (one column per year) into a normalized long format.

The output CSV is prepared for database ingestion and includes
basic error handling suitable for a beginner Big Data ETL project.
"""

import pandas as pd
from pathlib import Path
import time


# ======================================================
# PROJECT PATHS
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_raw"
CLEAN_DIR = BASE_DIR / "data_clean"

INPUT = DATA_DIR / "eustat_population_nationality.csv"
OUTPUT = CLEAN_DIR / "eustat_population_nationality_clean.csv"

CLEAN_DIR.mkdir(exist_ok=True)


# ======================================================
# MAIN TRANSFORMATION
# ======================================================

def run():

    print("\n====== TRANSFORM EUSTAT NATIONALITY ======")
    print("Reading input file:", INPUT)

    start = time.time()

    # --------------------------------------------------
    # READ CSV WITH BASIC ERROR HANDLING
    # --------------------------------------------------
    try:
        df = pd.read_csv(
            INPUT,
            sep=",",
            skiprows=1,            # skip long title row
            encoding="utf-8-sig",
            quotechar='"',
            dtype=str              # read everything as string first
        )
    except Exception as e:
        print("ERROR reading input CSV:", e)
        return

    print("Detected columns:", list(df.columns))
    print("Original rows:", len(df))

    # --------------------------------------------------
    # REQUIRED DIMENSION COLUMNS
    # --------------------------------------------------
    id_vars = [
        "province",
        "continent of nationality",
        "relation with the economic activity",
        "sex"
    ]

    missing = [c for c in id_vars if c not in df.columns]
    if missing:
        print("Missing required columns:", missing)
        print("Aborting transformation")
        return

    # --------------------------------------------------
    # DETECT YEAR COLUMNS
    # --------------------------------------------------
    year_cols = [col for col in df.columns if col.isdigit()]

    if not year_cols:
        print("No year columns detected (digit-only column names)")
        print("Aborting transformation")
        return

    print("Detected years:", sorted(year_cols))

    # --------------------------------------------------
    # WIDE TO LONG TRANSFORMATION
    # --------------------------------------------------
    df_long = df.melt(
        id_vars=id_vars,
        value_vars=year_cols,
        var_name="year",
        value_name="population"
    )

    # --------------------------------------------------
    # CLEAN NUMERIC VALUES
    # --------------------------------------------------
    df_long = df_long.dropna(subset=["population"])

    df_long["population"] = pd.to_numeric(
        df_long["population"],
        errors="coerce"
    )

    df_long = df_long.dropna(subset=["population"])
    df_long["year"] = df_long["year"].astype(int)

    # --------------------------------------------------
    # RENAME COLUMNS TO MATCH DATABASE SCHEMA
    # --------------------------------------------------
    df_long = df_long.rename(columns={
        "province": "province",
        "continent of nationality": "nationality",
        "relation with the economic activity": "relation_with_activity",
        "sex": "sex"
    })

    # --------------------------------------------------
    # ADD CONSTANT DIMENSION
    # --------------------------------------------------
    df_long["ccaa"] = "País Vasco"

    # --------------------------------------------------
    # FINAL COLUMN ORDER
    # --------------------------------------------------
    final_cols = [
        "ccaa",
        "province",
        "nationality",
        "relation_with_activity",
        "sex",
        "year",
        "population"
    ]

    df_long = df_long[final_cols]

    # --------------------------------------------------
    # SAVE OUTPUT
    # --------------------------------------------------
    df_long.to_csv(OUTPUT, index=False, encoding="utf-8-sig")

    print("Final rows:", len(df_long))
    print("Final columns:", list(df_long.columns))
    print("Sample (first 5 rows):")
    print(df_long.head())
    print("Saved to:", OUTPUT)
    print(f"OK ({round(time.time() - start, 2)}s)")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
