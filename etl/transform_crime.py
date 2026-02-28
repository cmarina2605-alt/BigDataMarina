"""
Crime detentions transformation script (PX 03008).

This script transforms the crime detentions dataset provided in PX format
(file 03008.px) into a clean and flat CSV file.

The dataset contains crime detention counts classified by:
- Province
- Region (continent or geographic area)
- Nationality (country)
- Sex
- Year

The original PX file encodes region and nationality in a single hierarchical
dimension, which requires custom parsing logic.
"""

from pyaxis import pyaxis
import pandas as pd
from pathlib import Path
import re


# ======================================================
# PROJECT PATHS
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_raw"
CLEAN_DIR = BASE_DIR / "data_clean"

RAW = DATA_DIR / "crime_detentions.px"
OUT = CLEAN_DIR / "crime_detentions_clean.csv"

CLEAN_DIR.mkdir(exist_ok=True, parents=True)


# ======================================================
# AUXILIARY FUNCTIONS
# ======================================================

def clean_name(x):
    """
    Removes numeric prefixes from region or nationality names.

    Examples:
    - '1.- Europe'    -> 'Europe'
    - '1.01.- France' -> 'France'
    """
    if pd.isna(x):
        return ""
    return re.sub(r"^\d+(\.\d+)?\.-\s*", "", str(x)).strip()


# ======================================================
# MAIN TRANSFORMATION
# ======================================================

def run():
    """
    Transforms the crime detentions PX file into a clean CSV file.

    The function:
    - Parses the PX file using pyaxis
    - Renames columns based on the real dataset structure
    - Separates region and nationality from a hierarchical text field
    - Cleans numeric values and converts them to integers
    - Saves the final result as a CSV file
    """

    print("\n====== TRANSFORM CRIME PX (03008) ======")

    px = pyaxis.parse(str(RAW), encoding="latin-1")
    df = px["DATA"].copy()

    # Rename columns according to dataset semantics
    df = df.rename(columns={
        "Provincias": "province",
        "Nacionalidad": "raw_nat",
        "Sexo": "sex",
        "periodo": "year",
        "DATA": "value"
    })

    # --------------------------------------------------
    # SPLIT REGION AND NATIONALITY
    # --------------------------------------------------
    current_region = None
    regions = []
    nationalities = []

    for val in df["raw_nat"]:
        text = str(val)

        # Region (e.g. continent)
        if re.match(r"^\d+\.-", text):
            current_region = clean_name(text)
            regions.append(current_region)
            nationalities.append(None)

        # Country
        elif re.match(r"^\d+\.\d+\.-", text):
            regions.append(current_region)
            nationalities.append(clean_name(text))

        else:
            regions.append(current_region)
            nationalities.append(None)

    df["region"] = regions
    df["nationality"] = nationalities

    # Keep only country-level rows
    df = df[df["nationality"].notna()]

    # --------------------------------------------------
    # CLEAN NUMERIC VALUES
    # --------------------------------------------------
    df["value"] = (
        df["value"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    # Remove invalid rows
    df = df.dropna(subset=["value", "year"])

    # Convert to integer types
    df["year"] = df["year"].astype(int)
    df["value"] = df["value"].astype(int)

    # --------------------------------------------------
    # FINAL COLUMN SELECTION AND SORTING
    # --------------------------------------------------
    df = df[
        ["province", "region", "nationality", "sex", "year", "value"]
    ].sort_values(["province", "region", "nationality", "year"])

    # Save CSV using UTF-8 with BOM for compatibility
    df.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("Rows:", len(df))
    print("Provinces:", df["province"].nunique())
    print("Regions:", df["region"].nunique())
    print("Nationalities:", df["nationality"].nunique())
    print("OK")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
