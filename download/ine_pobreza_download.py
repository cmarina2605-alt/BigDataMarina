"""
INE poverty risk data download script (JSON format).

This script downloads data related to the risk of poverty
(Encuesta de Condiciones de Vida - ECV) from the INE public API.

The dataset corresponds to table 49148 and is retrieved in JSON format.
The downloaded data is stored locally for later transformation and
database ingestion.
"""

import requests
import json
from pathlib import Path


# ======================================================
# DATA SOURCE
# ======================================================

URL = "https://servicios.ine.es/wstempus/js/es/DATOS_TABLA/49148"


# ======================================================
# PROJECT PATHS
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data_raw"
RAW_DIR.mkdir(exist_ok=True)

OUT = RAW_DIR / "ine_pobreza.json"


# ======================================================
# MAIN EXECUTION
# ======================================================

def run():
    """
    Downloads the INE poverty risk dataset and stores it as a JSON file.

    The function performs a direct HTTP request to the INE API and
    saves the response using UTF-8 encoding to preserve special
    characters.
    """

    print("Downloading INE poverty risk data (table 49148)...")

    r = requests.get(URL, timeout=60)
    r.raise_for_status()

    data = r.json()

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("OK ->", OUT)


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
