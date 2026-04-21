"""
Transform INE Padrón Continuo — foreign population by province.

Reads the raw CSV downloaded from INE (all provinces, all
nationalities) and filters it to the three Basque Country provinces:
Araba/Álava, Bizkaia, and Gipuzkoa.

Only the "TOTAL EXTRANJEROS" (total foreigners) nationality category
and "Ambos sexos" (both sexes) are kept, producing one row per
province per year with the total foreign population count.

Output: data_clean/ine_foreign_population_province.csv
"""

import csv
import io
from pathlib import Path


# ======================================================
# PATH CONFIGURATION
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data_raw"
CLEAN_DIR = BASE_DIR / "data_clean"
CLEAN_DIR.mkdir(exist_ok=True)

INPUT_FILE = RAW_DIR / "ine_padron_foreign_province.csv"
OUTPUT_FILE = CLEAN_DIR / "ine_foreign_population_province.csv"


# ======================================================
# PROVINCE MAPPING
# ======================================================

BASQUE_PROVINCES = {
    "01 Araba/Álava": ("Araba/Álava", "Araba"),
    "48 Bizkaia":     ("Bizkaia",     "Bizkaia"),
    "20 Gipuzkoa":    ("Gipuzkoa",    "Gipuzkoa"),
}


# ======================================================
# TRANSFORM FUNCTION
# ======================================================

def run():
    """
    Filters the raw INE CSV to Basque provinces and writes
    a clean CSV with standardised column names.
    """

    print("\n====== TRANSFORM INE PADRÓN FOREIGN ======")

    with open(INPUT_FILE, "r", encoding="utf-8-sig") as f:
        raw = f.read()

    reader = csv.reader(io.StringIO(raw), delimiter=";")
    next(reader)  # skip header

    rows = []
    for row in reader:
        if len(row) < 5:
            continue

        nationality = row[0].strip()
        province = row[1].strip()
        sex = row[2].strip()
        year = row[3].strip()
        total = row[4].strip()

        if (
            province in BASQUE_PROVINCES
            and sex == "Ambos sexos"
            and nationality == "TOTAL EXTRANJEROS"
        ):
            prov_name, prov_std = BASQUE_PROVINCES[province]
            total_clean = total.replace(".", "").replace(",", ".")
            try:
                val = int(float(total_clean))
            except ValueError:
                continue

            rows.append({
                "province": prov_name,
                "province_std": prov_std,
                "year": int(year),
                "foreign_population": val,
            })

    rows.sort(key=lambda x: (x["province_std"], x["year"]))

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["province", "province_std", "year", "foreign_population"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows)} rows to {OUTPUT_FILE.name}")
    print("TRANSFORM INE PADRÓN FOREIGN completed")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
