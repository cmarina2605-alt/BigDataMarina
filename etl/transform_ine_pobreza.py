"""
INE poverty risk (ECV) transformation script.

This script transforms the INE poverty risk dataset from JSON format
into a clean CSV file encoded in UTF-8 with BOM.

Each output row represents a poverty indicator value by:
- territory (CCAA)
- year
- indicator
"""

import json
import pandas as pd
from pathlib import Path


# ======================================================
# PROJECT PATHS
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data_raw"
CLEAN_DIR = BASE_DIR / "data_clean"
CLEAN_DIR.mkdir(exist_ok=True)

RAW = RAW_DIR / "ine_pobreza.json"
OUT = CLEAN_DIR / "ine_pobreza.csv"


# ======================================================
# MAIN TRANSFORMATION
# ======================================================

def run():

    print("Transforming INE poverty JSON...")

    # --------------------------------------------------
    # LOAD RAW JSON WITH BASIC ERROR HANDLING
    # --------------------------------------------------
    try:
        with open(RAW, encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        print("ERROR loading JSON file:", e)
        return

    rows = []
    skipped_series = 0

    # --------------------------------------------------
    # ITERATE OVER SERIES
    # --------------------------------------------------
    for idx, serie in enumerate(raw):

        try:
            nombre = serie.get("Nombre", "").strip()

            # Example:
            # "Andalucía. Todas las edades. Tasa de riesgo de pobreza ..."
            territory = nombre.split(".")[0].strip()
            indicator = nombre.split(". ", 1)[1] if ". " in nombre else None

            for d in serie.get("Data", []):
                rows.append({
                    "level": "ccaa",
                    "territory": territory,
                    "year": d.get("Anyo"),
                    "indicator": indicator,
                    "poverty_rate": d.get("Valor"),
                })

        except Exception as e:
            # Skip only the problematic series
            print(f"Warning: error processing series {idx}: {e}")
            skipped_series += 1
            continue

    if not rows:
        print("No rows generated → aborting")
        return

    # --------------------------------------------------
    # BUILD DATAFRAME AND CLEAN NUMERIC VALUES
    # --------------------------------------------------
    df = pd.DataFrame(rows)

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["poverty_rate"] = pd.to_numeric(df["poverty_rate"], errors="coerce")

    df = df.dropna(subset=["year", "poverty_rate"])

    # --------------------------------------------------
    # SAVE OUTPUT CSV
    # --------------------------------------------------
    df.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("Rows generated:", len(df))
    print("Series skipped due to errors:", skipped_series)
    print("Saved to:", OUT)
    print("OK")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
