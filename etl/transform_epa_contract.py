"""
Transform EPA contract JSON to flat CSV.

Output columns:
- sex
- age_group
- indicator
- disability
- year
- value

This version includes basic error handling suitable for a beginner
Big Data ETL project.
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

RAW = DATA_DIR / "epa_contract.json"
OUT = CLEAN_DIR / "epa_contract_clean.csv"

CLEAN_DIR.mkdir(exist_ok=True)


# ======================================================
# SAFE NUMERIC CONVERSION
# ======================================================

def safe_float(x):
    """
    Safely converts a value to float.

    Returns None if the value is invalid or cannot be converted.
    """
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


# ======================================================
# MAIN TRANSFORMATION
# ======================================================

def run():

    print("\n====== TRANSFORM EPA CONTRACT ======")

    # --------------------------------------------------
    # Load raw JSON with basic error handling
    # --------------------------------------------------
    try:
        with open(RAW, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print("ERROR loading JSON file:", e)
        return

    rows = []
    discarded = 0

    # --------------------------------------------------
    # Iterate over series
    # --------------------------------------------------
    for idx, serie in enumerate(data):

        try:
            meta = {
                m["T3_Variable"]: m["Nombre"]
                for m in serie["MetaData"]
            }

            for d in serie["Data"]:

                value = safe_float(d.get("Valor"))

                # Skip invalid numeric values
                if value is None:
                    discarded += 1
                    continue

                rows.append({
                    "sex": meta.get("sexo"),
                    "age_group": meta.get("grupo de edad"),
                    "indicator": meta.get("tipo de indicador"),
                    "disability": meta.get("personas sin y con discapacidad"),
                    "year": int(d.get("NombrePeriodo")),
                    "value": value
                })

        except Exception as e:
            # Skip only the problematic series, not the full ETL
            print(f"Warning: error processing series {idx}: {e}")
            continue

    if not rows:
        print("No valid rows generated → aborting")
        return

    # --------------------------------------------------
    # Build DataFrame and save CSV
    # --------------------------------------------------
    df = pd.DataFrame(rows)

    df.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("Valid rows:", len(df))
    print("Discarded rows (invalid values):", discarded)
    print("Saved to:", OUT)
    print("OK")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()

