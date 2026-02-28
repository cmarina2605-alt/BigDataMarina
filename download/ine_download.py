"""
INE datasets download script (JSON format).

This script downloads multiple datasets from the INE public API
using their table identifiers (TID). Each dataset is retrieved
in JSON format and stored locally.

The script uses streaming downloads to handle large files and
provides basic progress feedback during the download process.
"""

import requests
import json
import time
from pathlib import Path


# ======================================================
# FIXED PROJECT PATH (bigdata/)
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_raw"
DATA_DIR.mkdir(exist_ok=True)

print("Saving files to:", DATA_DIR.resolve())


# ======================================================
# INE TABLES (VALID TIDs)
# ======================================================

TABLAS = {
    "population_ccaa_nationality": "31304",   # CCAA by nationality
    "population_birth_country": "31307",      # country of birth
    "epa_contract": "8527"                    # contract type (EPA)
}

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


# ======================================================
# STREAMING DOWNLOAD WITH PROGRESS
# ======================================================

def download(tabla_id, name):
    """
    Downloads a single INE dataset using its table identifier (TID).

    The dataset is downloaded in streaming mode to avoid loading
    large files into memory. A simple progress indicator is printed
    based on the number of bytes downloaded.
    """

    url = f"https://servicios.ine.es/wstempus/js/es/DATOS_TABLA/{tabla_id}?tip=AM"

    print(f"\nDownloading {name} (TID={tabla_id})")

    output = DATA_DIR / f"{name}.json"
    start = time.time()

    try:
        with requests.get(
            url,
            headers=HEADERS,
            stream=True,
            timeout=60
        ) as r:
            r.raise_for_status()

            total = 0

            with open(output, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        total += len(chunk)
                        print(
                            f"{round(total / 1024 / 1024, 1)} MB downloaded...",
                            end="\r"
                        )

        elapsed = round(time.time() - start, 2)
        print(f"\nOK {name} ({elapsed}s)")

    except Exception as e:
        # Generic exception handling is intentionally kept
        # to allow the script to continue downloading other datasets
        print(f"\nERROR {name}: {e}")


# ======================================================
# MAIN EXECUTION
# ======================================================

def run():
    """
    Iterates over the configured INE tables and downloads
    each dataset sequentially.
    """

    print("\n====== INE DOWNLOAD ======")

    for name, tid in TABLAS.items():
        download(tid, name)

    print("\nINE download completed")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
