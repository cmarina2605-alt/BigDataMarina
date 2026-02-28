"""
INE population by birth country transformation script (JSON to CSV).

This script transforms a JSON dataset from the INE API into a flat CSV file.
The input JSON may have different structures (list or dictionary), so the
script automatically detects the correct format.

The output CSV contains one row per observation with the following fields:
- region
- indicator
- sex
- age_group
- nationality
- periodicity
- data_type
- year
- value
- unit
"""

import json
import pandas as pd
from pathlib import Path


# ======================================================
# PROJECT PATHS
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_raw"
CLEAN_DIR = BASE_DIR / "data_clean"

RAW = DATA_DIR / "population_birth_country.json"
OUT = CLEAN_DIR / "birth_country_clean.csv"

CLEAN_DIR.mkdir(exist_ok=True)


# ======================================================
# SAFE NUMERIC CONVERSION
# ======================================================

def safe_float(x):
    """
    Safely converts a value to float.

    If the conversion fails, None is returned instead of raising
    an exception. This avoids breaking the transformation when
    non-numeric values are encountered.
    """
    try:
        return float(x)
    except:
        return None


# ======================================================
# MAIN TRANSFORMATION
# ======================================================

def run():
    """
    Transforms the INE JSON file into a clean, flat CSV file.

    The function:
    - Loads the raw JSON file
    - Detects whether the structure is a dictionary or a list
    - Extracts metadata and data values
    - Builds a list of normalized rows
    - Saves the result as a CSV file
    """

    print("\n====== TRANSFORM INE BIRTH COUNTRY ======")

    with open(RAW, encoding="utf-8") as f:
        raw = json.load(f)

    # Detect JSON structure automatically
    if isinstance(raw, dict) and "Data" in raw:
        series_list = raw["Data"]
    else:
        # The JSON is already a list of series
        series_list = raw

    rows = []

    for serie in series_list:

        # Extract metadata into a dictionary for easy access
        meta = {
            m["T3_Variable"]: m["Nombre"]
            for m in serie.get("MetaData", [])
        }

        unit = serie.get("T3_Unidad")

        for d in serie.get("Data", []):

            value = safe_float(d.get("Valor"))
            if value is None:
                continue

            rows.append({
                "region": meta.get("Comunidades y Ciudades Autónomas"),
                "indicator": meta.get("Conceptos demógraficos"),
                "sex": meta.get("Sexo"),
                "age_group": meta.get("Totales de edad"),
                "nationality": meta.get("Nacionalidad"),
                "periodicity": meta.get("Periodicidad"),
                "data_type": meta.get("Tipo de dato"),
                "year": int(d.get("Anyo")),
                "value": value,
                "unit": unit
            })

    df = pd.DataFrame(rows)

    print("Generated rows:", len(df))

    # Save CSV using UTF-8 with BOM for compatibility
    df.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("Saved to:", OUT)
    print("OK")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
