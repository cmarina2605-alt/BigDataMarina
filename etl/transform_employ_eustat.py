"""
EUSTAT activity rate transformation script (wide to long).

This script transforms the EUSTAT activity by nationality dataset
from a wide CSV format into a normalized long format suitable
for analysis and database ingestion.

The input file contains multiple year columns that are dynamically
detected and reshaped into a single 'year' and 'value' column.
"""

import pandas as pd
from pathlib import Path


# ======================================================
# PROJECT PATHS
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
RAW = BASE_DIR / "data_raw/eustat_activity_nationality.csv"
OUT = BASE_DIR / "data_clean/eustat_activity_nationality_clean.csv"

OUT.parent.mkdir(exist_ok=True, parents=True)


# ======================================================
# MAIN TRANSFORMATION
# ======================================================

def run():
    """
    Transforms the EUSTAT activity rate CSV file into a clean
    long-format CSV file.

    The function:
    - Reads the raw CSV file
    - Renames the first columns to meaningful dimension names
    - Detects year columns automatically
    - Converts the dataset from wide to long format
    - Cleans numeric values
    - Saves the transformed data as a CSV file
    """

    print("\n===== TRANSFORM EUSTAT ACTIVITY =====")

    # --------------------------------------------------
    # READ CSV FILE
    # --------------------------------------------------
    # The header row starts at index 1 due to the structure
    # of the original EUSTAT CSV file.
    df = pd.read_csv(
        RAW,
        sep=",",
        header=1,
        dtype=str,
        encoding="latin-1"
    )

    print("Original columns:", df.columns.tolist())

    # --------------------------------------------------
    # RENAME DIMENSION COLUMNS
    # --------------------------------------------------
    df = df.rename(columns={
        df.columns[0]: "rate_type",
        df.columns[1]: "province",
        df.columns[2]: "nationality",
        df.columns[3]: "quarter"
    })

    # --------------------------------------------------
    # DETECT YEAR COLUMNS AUTOMATICALLY
    # --------------------------------------------------
    year_cols = [c for c in df.columns if c.isdigit()]

    print("Detected years:", year_cols)

    # --------------------------------------------------
    # WIDE TO LONG TRANSFORMATION
    # --------------------------------------------------
    # This step creates a single 'value' column
    df = df.melt(
        id_vars=["rate_type", "province", "nationality", "quarter"],
        value_vars=year_cols,
        var_name="year",
        value_name="value"
    )

    # --------------------------------------------------
    # CLEAN NUMERIC VALUES
    # --------------------------------------------------
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    df["value"] = (
        df["value"]
        .astype(str)
        .str.replace(",", ".", regex=False)
    )

    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Remove rows without valid numeric values
    df = df.dropna(subset=["value"])

    # --------------------------------------------------
    # SORT FINAL DATASET
    # --------------------------------------------------
    df = df.sort_values(["year", "province", "rate_type"])

    # Save CSV using UTF-8 with BOM for compatibility
    df.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("Rows:", len(df))
    print("Saved to:", OUT)
    print("OK")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
