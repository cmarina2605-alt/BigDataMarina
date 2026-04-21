"""
Transform INE Estadística Continua de Población (ECP) — foreign population.

Reads the raw CSV downloaded from the INE tempus API and computes the
foreign population percentage per province per year.  Only years up to
2025 are kept (2026 may be provisional/incomplete).

Output: data_clean/ine_ecp_foreign_population_province.csv
"""

import csv
from pathlib import Path


# ======================================================
# PATH CONFIGURATION
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data_raw"
CLEAN_DIR = BASE_DIR / "data_clean"
CLEAN_DIR.mkdir(exist_ok=True)

INPUT_FILE = RAW_DIR / "ine_ecp_foreign_population.csv"
OUTPUT_FILE = CLEAN_DIR / "ine_ecp_foreign_population_province.csv"


# ======================================================
# TRANSFORM FUNCTION
# ======================================================

def run():
    """
    Reads raw ECP data, computes foreign_population_pct, filters
    years <= 2025, and writes the clean CSV.
    """

    print("\n====== TRANSFORM INE ECP FOREIGN ======")

    if not INPUT_FILE.exists():
        print(f"Input file not found: {INPUT_FILE}")
        print("Run the download step first.")
        return

    rows = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            year = int(row["year"])
            if year > 2025:
                continue

            foreign_pop = int(row["foreign_population"])
            total_pop = int(row["total_population"])
            pct = round(foreign_pop / total_pop * 100, 2) if total_pop > 0 else 0.0

            rows.append({
                "province_std": row["province_std"],
                "year": year,
                "foreign_population": foreign_pop,
                "total_population": total_pop,
                "foreign_population_pct": pct,
            })

    rows.sort(key=lambda x: (x["province_std"], x["year"]))

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["province_std", "year", "foreign_population",
                        "total_population", "foreign_population_pct"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows)} rows to {OUTPUT_FILE.name}")
    print("TRANSFORM INE ECP FOREIGN completed")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
