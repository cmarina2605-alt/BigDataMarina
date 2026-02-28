"""
INE employment data download script (JSON format).

This script downloads employment-related data from the INE public API
and stores the response locally in JSON format.

The dataset corresponds to table 65992 and is retrieved using a
direct HTTP request.
"""

import requests
from pathlib import Path
import time


# ======================================================
# PATH CONFIGURATION
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
OUT = BASE_DIR / "data_raw/ine_employ.json"
OUT.parent.mkdir(exist_ok=True)


# ======================================================
# DATA SOURCE
# ======================================================

URL = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/65992?tip=AM"


# ======================================================
# DOWNLOAD LOGIC
# ======================================================

def run():
    """
    Downloads employment data from the INE API and saves it as a JSON file.

    Basic error handling is implemented to manage HTTP and network errors.
    """

    print("\n===== DOWNLOAD INE EMPLOYMENT DATA (JSON) =====")
    start = time.time()

    try:
        response = requests.get(URL, timeout=60)
        response.raise_for_status()

        OUT.write_bytes(response.content)

    except requests.exceptions.RequestException as e:
        print("Failed to download INE employment data.")
        print("Error message:", e)
        return

    elapsed = round(time.time() - start, 2)

    # Basic validation of downloaded content
    if OUT.stat().st_size == 0:
        print("Downloaded file is empty.")
        return

    print(f"File successfully saved at {OUT} ({elapsed}s)")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
