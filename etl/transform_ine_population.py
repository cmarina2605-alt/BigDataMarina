"""
INE population by territory transformation script.

This script transforms the INE population JSON dataset into a flat CSV file
ready for PostgreSQL ingestion.

The output dataset contains population values classified by:
- geographic level (province, CCAA, national)
- territory
- year
- sex
- age group
- nationality
- demographic concept
- data type and reference period

The output CSV is also split into three parts to avoid COPY timeouts
when loading large volumes of data into Supabase/PostgreSQL.
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

RAW_JSON = DATA_DIR / "population_ccaa_nationality.json"

OUT_FULL = CLEAN_DIR / "population_territory.csv"
OUT_P1 = CLEAN_DIR / "population_territory_part1.csv"
OUT_P2 = CLEAN_DIR / "population_territory_part2.csv"
OUT_P3 = CLEAN_DIR / "population_territory_part3.csv"

CLEAN_DIR.mkdir(exist_ok=True)


# ======================================================
# METADATA EXTRACTION
# ======================================================

def extract_metadata(meta_list):
    """
    Extracts relevant metadata fields from the INE metadata list.
    """

    meta = {}

    for item in meta_list:
        var = item.get("T3_Variable", "").strip()
        nombre = item.get("Nombre", "").strip()

        if not var or not nombre:
            continue

        var_norm = (
            var.lower()
            .replace(" ", "_")
            .replace("í", "i")
            .replace("ó", "o")
        )

        if "sexo" in var_norm:
            meta["sex"] = nombre

        elif any(k in var_norm for k in ["edad", "edades", "grupo_etario"]):
            if "total" not in nombre.lower():
                meta["age_group"] = nombre
            elif "age_group" not in meta:
                meta["age_group"] = "Todas las edades"

        elif "nacionalidad" in var_norm:
            meta["nationality"] = nombre

        elif "comunidades" in var_norm or "ciudades autonomas" in var_norm:
            meta["ccaa"] = nombre

        elif "provincias" in var_norm:
            meta["province"] = nombre

        elif "total nacional" in var_norm:
            meta["national"] = nombre

        elif "conceptos_demograficos" in var_norm:
            meta["concept"] = nombre

        elif "tipo_de_dato" in var_norm:
            meta["data_type"] = nombre

        elif "unidad" in var_norm:
            meta["unit"] = nombre

    if "province" in meta:
        meta["level"] = "province"
        meta["territory"] = meta["province"]
    elif "ccaa" in meta:
        meta["level"] = "ccaa"
        meta["territory"] = meta["ccaa"]
    elif "national" in meta:
        meta["level"] = "national"
        meta["territory"] = "España"
    else:
        meta["level"] = "national"
        meta["territory"] = "España"

    return meta


# ======================================================
# MAIN TRANSFORMATION
# ======================================================

def run():

    print("\n====== TRANSFORM INE POPULATION ======")
    print("Reading:", RAW_JSON)

    try:
        with open(RAW_JSON, encoding="utf-8") as f:
            data = json.load(f)
        print(f"JSON loaded. Number of series: {len(data)}")
    except Exception as e:
        print("ERROR reading JSON file:", e)
        return

    rows = []
    skipped_points = 0

    for idx, serie in enumerate(data):
        try:
            meta = extract_metadata(serie.get("MetaData", []))
            datapoints = serie.get("Data", [])

            for punto in datapoints:
                year_str = punto.get("Anyo")
                valor = punto.get("Valor")

                if year_str is None or valor is None:
                    skipped_points += 1
                    continue

                try:
                    year = int(year_str)
                    population = float(valor)
                except (ValueError, TypeError):
                    skipped_points += 1
                    continue

                rows.append({
                    "level": meta.get("level", "unknown"),
                    "territory": meta.get("territory", "Desconocido"),
                    "year": year,
                    "population": population,
                    "sex": meta.get("sex"),
                    "age_group": meta.get("age_group", "Todas las edades"),
                    "nationality": meta.get("nationality"),
                    "concept": meta.get("concept", "Población"),
                    "data_type": meta.get("data_type", "Número"),
                    "unit": meta.get("unit", "Personas"),
                    "tipo_dato": punto.get("T3_TipoDato", ""),
                    "periodo": punto.get("T3_Periodo", "")
                })

        except Exception as e:
            print(f"Warning: error processing series {idx}: {e}")
            continue

    if not rows:
        print("No valid data found → aborting")
        return

    df = pd.DataFrame(rows).sort_values(
        ["level", "territory", "year", "sex", "age_group", "nationality", "periodo"]
    )

    # --------------------------------------------------
    # SAVE FULL CSV (optional but recommended)
    # --------------------------------------------------
    df.to_csv(OUT_FULL, index=False, encoding="utf-8-sig")

    # --------------------------------------------------
    # SPLIT INTO 3 PARTS FOR POSTGRES LOAD
    # --------------------------------------------------
    n = len(df)
    chunk = n // 3

    df.iloc[:chunk].to_csv(OUT_P1, index=False, encoding="utf-8-sig")
    df.iloc[chunk:2*chunk].to_csv(OUT_P2, index=False, encoding="utf-8-sig")
    df.iloc[2*chunk:].to_csv(OUT_P3, index=False, encoding="utf-8-sig")

    print(f"Final rows: {n:,}")
    print("CSV files generated:")
    print(" -", OUT_P1.name)
    print(" -", OUT_P2.name)
    print(" -", OUT_P3.name)
    print("OK transformation completed")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
