"""
INE Estadística Continua de Población (ECP) — download script.

Downloads quarterly population estimates by province and nationality
from the INE tempus API.  The ECP replaced the Padrón Continuo from
2021 onwards and provides more frequent (quarterly) estimates with a
broader estimation methodology.

Each province needs two series: total population and foreign population.
The raw JSON responses are parsed and saved as a single CSV in data_raw/.
"""

import json
import time
import urllib.request
from pathlib import Path


# ======================================================
# FIXED PROJECT PATH
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_raw"
DATA_DIR.mkdir(exist_ok=True)


# ======================================================
# DOWNLOAD CONFIGURATION
# ======================================================

API_BASE = "https://servicios.ine.es/wstempus/js/ES/DATOS_SERIE"

# Series codes for total and foreign population per province.
# Each entry: (province_std, total_series_cod, foreign_series_cod)
SERIES = [
    ("Araba",    "ECP354988", "ECP354868"),
    ("Bizkaia",  "ECP353908", "ECP353788"),
    ("Gipuzkoa", "ECP352108", "ECP351988"),
]

HEADERS = {"User-Agent": "Mozilla/5.0"}
OUTPUT_NAME = "ine_ecp_foreign_population"


# ======================================================
# HELPER — FETCH A SINGLE SERIES
# ======================================================

def _fetch_series(cod: str) -> list[dict]:
    """
    Fetches the last 20 observations of a series from the INE
    tempus API and returns a list of {year, value} dicts.
    """

    url = f"{API_BASE}/{cod}?nult=20"
    req = urllib.request.Request(url, headers=HEADERS)
    resp = urllib.request.urlopen(req, timeout=60)
    payload = json.loads(resp.read().decode("utf-8"))

    records = []
    for item in payload.get("Data", []):
        # Anyo field contains the year
        year = item.get("Anyo")
        value = item.get("Valor")
        if year is not None and value is not None:
            records.append({"year": int(year), "value": value})

    return records


# ======================================================
# DOWNLOAD FUNCTION
# ======================================================

def download():
    """
    Downloads ECP series for all three Basque provinces and saves
    the combined result as a CSV.
    """

    import csv

    output = DATA_DIR / f"{OUTPUT_NAME}.csv"
    print(f"\nDownloading INE ECP population data...")

    start = time.time()
    rows = []

    for province_std, total_cod, foreign_cod in SERIES:
        print(f"  Fetching {province_std}...")

        try:
            total_data = _fetch_series(total_cod)
            time.sleep(1)  # be polite to the API
            foreign_data = _fetch_series(foreign_cod)
            time.sleep(1)

            # Index foreign data by year for easy lookup
            foreign_by_year = {r["year"]: r["value"] for r in foreign_data}

            for rec in total_data:
                yr = rec["year"]
                total_pop = rec["value"]
                foreign_pop = foreign_by_year.get(yr)

                if foreign_pop is not None:
                    rows.append({
                        "province_std": province_std,
                        "year": yr,
                        "foreign_population": int(round(foreign_pop)),
                        "total_population": int(round(total_pop)),
                    })

            print(f"    OK {province_std}: {len(total_data)} total, "
                  f"{len(foreign_data)} foreign observations")

        except Exception as e:
            print(f"    ERROR {province_std}: {e}")

    # Write CSV
    rows.sort(key=lambda x: (x["province_std"], x["year"]))

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["province_std", "year", "foreign_population",
                        "total_population"],
        )
        writer.writeheader()
        writer.writerows(rows)

    elapsed = round(time.time() - start, 2)
    print(f"OK {OUTPUT_NAME} ({len(rows)} rows, {elapsed}s)")


# ======================================================
# MAIN EXECUTION
# ======================================================

def run():
    """Download the INE ECP foreign-population series."""

    print("\n====== INE ECP FOREIGN DOWNLOAD ======")
    download()
    print("\nINE ECP download completed")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
