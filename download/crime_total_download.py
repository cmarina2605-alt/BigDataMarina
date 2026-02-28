"""
Total crime data download script (PX format).

This script downloads aggregated crime investigation data from the
Spanish Ministry of the Interior statistics portal. The dataset is
provided in PX format and contains total crime investigation figures.

The file is downloaded using streaming to efficiently handle
large files without loading them entirely into memory.
"""

import requests
import time
from pathlib import Path

# ======================================================
# PATH CONFIGURATION
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_raw"
DATA_DIR.mkdir(exist_ok=True)

# ======================================================
# DATA SOURCE
# ======================================================

URL = (
    "https://estadisticasdecriminalidad.ses.mir.es/"
    "sec/jaxiPx/files/_px/es/px/Datos3/l0/03009.px?nocab=1"
)

OUT = DATA_DIR / "crime_total.px"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ======================================================
# DOWNLOAD LOGIC
# ======================================================

def run():
    """
    Downloads the PX file containing total crime investigation data
    and stores it locally.

    Basic error handling is implemented to manage network issues
    and HTTP errors.
    """

    print("\n====== CRIME TOTAL DOWNLOAD (PX FORMAT) ======")

    start = time.time()
    total_bytes = 0

    try:
        response = requests.get(
            URL,
            headers=HEADERS,
            stream=True,
            timeout=60
        )

        # Raise exception for HTTP errors (4xx or 5xx)
        response.raise_for_status()

        with open(OUT, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)
                    print(
                        f"{round(total_bytes / 1024 / 1024, 1)} MB downloaded...",
                        end="\r"
                    )

    except requests.exceptions.RequestException as e:
        print("\nDownload failed due to a network or HTTP error.")
        print("Error message:", e)
        return

    # Basic validation of downloaded content
    if total_bytes == 0:
        print("\nDownload completed but the file is empty.")
        return

    elapsed = round(time.time() - start, 2)
    size_mb = round(total_bytes / 1024 / 1024, 2)

    print(
        f"\nDownload completed successfully: {OUT} "
        f"({size_mb} MB in {elapsed}s)"
    )


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
