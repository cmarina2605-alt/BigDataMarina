"""
INE Padrón Continuo — foreign population by province download script.

Downloads the foreign population data from INE's Estadística del
Padrón Continuo (table 03005).  This source provides annual counts
of registered foreign residents by province since 1998, filling the
gaps left by the EUSTAT nationality breakdown (which only covers
2010 and 2015-2024).

The data is downloaded as a semicolon-delimited CSV and saved in its
raw form to data_raw/ for subsequent transformation.
"""

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

URL = (
    "https://www.ine.es/jaxi/files/_px/es/csv_bdsc/"
    "t20/e245/p08/l0/03005.px?nocab=1"
)

HEADERS = {"User-Agent": "Mozilla/5.0"}
OUTPUT_NAME = "ine_padron_foreign_province"


# ======================================================
# DOWNLOAD FUNCTION
# ======================================================

def download():
    """
    Downloads the INE Padrón Continuo foreign-population CSV.

    The file is large (~22 MB) because it contains all provinces,
    nationalities and years.  The transformation step filters it
    down to the three Basque provinces.
    """

    output = DATA_DIR / f"{OUTPUT_NAME}.csv"
    print(f"\nDownloading INE Padrón foreign population data...")

    start = time.time()

    try:
        req = urllib.request.Request(URL, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=120)
        data = resp.read()

        with open(output, "wb") as f:
            f.write(data)

        elapsed = round(time.time() - start, 2)
        size_mb = round(len(data) / 1024 / 1024, 1)
        print(f"OK {OUTPUT_NAME} ({size_mb} MB, {elapsed}s)")

    except Exception as e:
        print(f"ERROR {OUTPUT_NAME}: {e}")


# ======================================================
# MAIN EXECUTION
# ======================================================

def run():
    """Download the INE Padrón Continuo foreign-population table."""

    print("\n====== INE PADRÓN FOREIGN DOWNLOAD ======")
    download()
    print("\nINE Padrón download completed")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
