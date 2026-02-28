"""
INE employment transformation script (table 65992).

This script transforms the raw JSON file downloaded from the INE API
into a clean, flat CSV file.

The dataset contains employment statistics classified by:
- Relation to activity
- Territory
- Sex
- Economic sector
- Nationality
- Year

Each observation is extracted from the JSON time series structure
and normalized into one row per year.
"""

import json
import pandas as pd
from pathlib import Path


# ======================================================
# PROJECT PATHS
# ======================================================

BASE = Path(__file__).resolve().parent.parent
RAW = BASE / "data_raw/ine_employ.json"
OUT = BASE / "data_clean/ine_employment_clean.csv"

OUT.parent.mkdir(exist_ok=True)


# ======================================================
# METADATA HELPER
# ======================================================

def meta_get(meta, key):
    """
    Searches a metadata list for a variable whose name contains
    the given key and returns its label.

    If no matching variable is found, None is returned.
    """
    for m in meta:
        if key in m["T3_Variable"]:
            return m["Nombre"]
    return None


# ======================================================
# MAIN TRANSFORMATION
# ======================================================

def run():
    """
    Transforms the INE employment JSON file into a clean CSV file.

    The function:
    - Loads the raw JSON file
    - Iterates through each time series
    - Extracts relevant metadata dimensions
    - Flattens yearly observations into rows
    - Saves the final dataset as a CSV file
    """

    print("\n===== TRANSFORM INE employ JSON =====")

    series = json.loads(RAW.read_text(encoding="utf-8"))

    rows = []

    for s in series:

        meta = s["MetaData"]

        # Extract dimensions from metadata
        relation = meta_get(meta, "RELACIÓN")
        territory = meta_get(meta, "Total Nacional")
        sex = meta_get(meta, "Sexo")
        sector = meta_get(meta, "SECTORES") or meta_get(meta, "CNAE")
        nationality = meta_get(meta, "Nacionalidad")

        # Extract yearly data points
        for d in s["Data"]:
            rows.append([
                relation,
                territory,
                sex,
                sector,
                nationality,
                d["Anyo"],
                d["Valor"]
            ])

    # Build DataFrame with explicit column order
    df = pd.DataFrame(rows, columns=[
        "relation_activity",
        "territory",
        "sex",
        "sector",
        "nationality",
        "year",
        "value"
    ])

    # Convert data types
    df["year"] = df["year"].astype(int)
    df["value"] = df["value"].astype(float)

    # Save CSV using UTF-8 with BOM for compatibility
    df.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("Rows:", len(df))
    print("Years:", sorted(df.year.unique()))
    print("Saved to:", OUT)
    print("OK")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
