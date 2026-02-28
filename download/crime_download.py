"""
Crime data download script (PX format).

This script downloads crime detention data from the Spanish Ministry
of the Interior statistics portal. The dataset is provided in PX format
and contains information about crime detentions involving foreign
nationals.

The file is downloaded using streaming to avoid loading large files
into memory.
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

# PX format (not XLSX or CSV)
URL = (
    "https://estadisticasdecriminalidad.ses.mir.es/"
    "sec/jaxiPx/files/_px/es/px/Datos3/l0/03008.px?nocab=1"
)

OUT = DATA_DIR / "crime_detentions.px"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ======================================================
# DOWNLOAD LOGIC
# ======================================================

def run():
    """
    Downloads the PX file and stores it locally.

    The download is performed using streaming to handle large files.
    Basic error handling is implemented to manage network issues
    and invalid responses.
    """

    print("\n====== CRIME DOWNLOAD (PX FORMAT) ======")

    start = time.time()
    total_bytes = 0

    try:
        response = requests.get(
            URL,
            headers=HEADERS,
            stream=True,
            timeout=60
        )

        # Raise an exception for HTTP errors (4xx / 5xx)
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

    # Basic validation of downloaded file
    if total_bytes == 0:
        print("\nDownload completed but file is empty.")
        return

    size_mb = round(total_bytes / 1024 / 1024, 2)
    elapsed = round(time.time() - start, 2)

    print(
        f"\nDownload completed successfully: {OUT} "
        f"({size_mb} MB in {elapsed}s)"
    )


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
