"""
Transform EUSTAT flat population CSV to a clean format for PostgreSQL.

Final output columns:
- province
- nationality
- year
- population

This script applies minimal error handling suitable for a beginner
Big Data ETL project.
"""

import pandas as pd
from pathlib import Path


# ======================================================
# PROJECT PATHS
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_raw"
CLEAN_DIR = BASE_DIR / "data_clean"

RAW = DATA_DIR / "eustat_population.csv"      # flat input CSV
OUT = CLEAN_DIR / "eustat_population_clean.csv"

CLEAN_DIR.mkdir(exist_ok=True)


# ======================================================
# MAIN TRANSFORMATION
# ======================================================

def run():

    print("\n====== TRANSFORM EUSTAT (FLAT) ======")
    print("Reading input file:", RAW)

    # ---------------------------------
    # READ INPUT CSV WITH BASIC CONTROL
    # ---------------------------------
    try:
        df = pd.read_csv(RAW)
    except Exception as e:
        print("ERROR reading input CSV:", e)
        return

    print("Original rows:", len(df))
    print("Original columns:", list(df.columns))

    # ---------------------------------
    # CHECK REQUIRED COLUMNS
    # ---------------------------------
    required_cols = ["provincia", "nacionalidad", "total"]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print("Missing required columns:", missing)
        print("Aborting transformation")
        return

    # ---------------------------------
    # CLEAN NUMERIC VALUES
    # ---------------------------------
    df["total"] = pd.to_numeric(df["total"], errors="coerce")

    # ---------------------------------
    # RENAME COLUMNS
    # ---------------------------------
    df = df.rename(columns={
        "provincia": "province",
        "nacionalidad": "nationality",
        "total": "population"
    })

    # ---------------------------------
    # ADD YEAR (FIXED VALUE)
    # ---------------------------------
    df["year"] = 2024

    # ---------------------------------
    # SELECT FINAL COLUMNS
    # ---------------------------------
    df = df[["province", "nationality", "year", "population"]]

    # ---------------------------------
    # SAVE OUTPUT CSV
    # ---------------------------------
    df.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("Final rows:", len(df))
    print("Saved to:", OUT)
    print("OK transform EUSTAT")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
