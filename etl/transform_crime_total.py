"""
Crime total investigations transformation script (PX to CSV).

This script transforms the crime investigation dataset provided
in PX format (file 03009.px) into a clean and flat CSV file.

The dataset contains aggregated crime investigation counts
classified by:
- Autonomous community
- Crime type
- Age group
- Sex
- Period (year)
"""

from pyaxis import pyaxis
import pandas as pd
from pathlib import Path


# ======================================================
# PROJECT PATHS
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_raw"
CLEAN_DIR = BASE_DIR / "data_clean"

RAW = DATA_DIR / "crime_total.px"
OUT = CLEAN_DIR / "crime_total_clean.csv"

CLEAN_DIR.mkdir(exist_ok=True, parents=True)


# ======================================================
# MAIN TRANSFORMATION
# ======================================================

def run():
    """
    Transforms the crime total PX file into a clean CSV file.

    The function:
    - Parses the PX file using pyaxis
    - Renames columns according to the real dataset dimensions
    - Cleans numeric values and converts them to integers
    - Filters invalid rows
    - Saves the result as a CSV file
    """

    print("\n===== TRANSFORM CRIME TOTAL (.px 03009) =====")

    px = pyaxis.parse(str(RAW), encoding="latin-1")
    df = px["DATA"].copy()

    print("Original columns:", df.columns.tolist())

    # --------------------------------------------------
    # REAL DIMENSION MAPPING (based on the actual PX file)
    # --------------------------------------------------
    df = df.rename(columns={
        "Comunidades autónomas": "territory",
        "Tipología penal": "crime_type",
        "Grupo de edad": "age_group",
        "Sexo": "sex",
        "periodo": "year",
        "DATA": "value"
    })

    # --------------------------------------------------
    # CLEAN NUMERIC VALUES
    # --------------------------------------------------
    # Replace thousand separators and decimal commas
    df["value"] = (
        df["value"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    # Remove rows with invalid numeric values
    df = df.dropna(subset=["value", "year"])

    # Convert to integer types
    df["year"] = df["year"].astype(int)
    df["value"] = df["value"].astype(int)

    # --------------------------------------------------
    # FINAL COLUMN SELECTION AND SORTING
    # --------------------------------------------------
    df = df[
        ["territory", "crime_type", "age_group", "sex", "year", "value"]
    ].sort_values(["territory", "crime_type", "year"])

    # Save CSV using UTF-8 with BOM for compatibility
    df.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("Rows:", len(df))
    print("Territories:", df["territory"].nunique())
    print("Crime types:", df["crime_type"].nunique())
    print("OK")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
