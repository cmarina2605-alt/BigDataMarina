"""
Housing data download script.

This script downloads housing price datasets from two different sources:
- EUSTAT (CSV format)
- Spanish Ministry of Transport (MIVAU) historical XLS file

The files are stored directly in the data_raw directory for later
processing and transformation.
"""

import requests
import pandas as pd
from pathlib import Path
from datetime import datetime


# ======================================================
# DATA DIRECTORY
# ======================================================

DATA_DIR = Path("data_raw")
DATA_DIR.mkdir(exist_ok=True)


# ======================================================
# HTTP HEADERS
# ======================================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


# ======================================================
# DOWNLOAD FUNCTION
# ======================================================

def descargar_archivo(url, nombre_base, es_csv=True):
    """
    Downloads a file from the given URL and saves it to disk.

    The function supports both CSV and XLS formats. The file is written
    directly to the data_raw directory without additional processing.
    """

    print(f"Downloading: {nombre_base} from {url}")

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()

        ext = "csv" if es_csv else "xls"
        filename = f"{nombre_base}.{ext}"
        file_path = DATA_DIR / filename

        file_path.write_bytes(r.content)

        print(f"File saved to: {file_path}")
        print(f"File size: {file_path.stat().st_size / 1024:.1f} KB")

        return file_path

    except Exception as e:
        # Generic exception handling is intentionally kept
        # to avoid stopping the execution if one source fails
        print(f"Error downloading {nombre_base}: {e}")
        return None


# ======================================================
# FILE PREVIEW (OPTIONAL)
# ======================================================

def previsualizar_csv_o_xls(file_path, es_csv=True):
    """
    Displays a preview of the downloaded CSV or XLS file.

    This function is intended for manual verification of the file
    structure and is not automatically executed in the pipeline.
    """

    if not file_path:
        return

    try:
        if es_csv:
            df = pd.read_csv(
                file_path,
                sep=",",
                decimal=",",
                encoding="utf-8",
                nrows=10
            )
        else:
            df = pd.read_excel(
                file_path,
                sheet_name=0,
                nrows=10,
                skiprows=0
            )

        print(f"\nPreview of {file_path.name} (first 10 rows):")
        print(df.head(10))
        print("\nDetected columns:", df.columns.tolist())

    except Exception as e:
        print(f"Could not preview {file_path.name}: {e}")
        print("Please open the file manually to inspect its structure.")


# ======================================================
# MAIN EXECUTION
# ======================================================

if __name__ == "__main__":

    print("===== DIRECT DOWNLOAD TO data_raw =====")
    print("Current date:", datetime.now().strftime("%Y-%m-%d"))

    # --------------------------------------------------
    # Source 1: EUSTAT - Quarterly housing prices (€/m²)
    # --------------------------------------------------
    url_eustat = "https://www.eustat.eus/elementos/xls0019433_c.csv"
    path_eustat = descargar_archivo(
        url_eustat,
        "eustat_precios_vivienda",
        es_csv=True
    )

    # --------------------------------------------------
    # Source 2: MIVAU - Historical appraisal value (€/m²)
    # --------------------------------------------------
    url_mivau_general = (
        "https://apps.fomento.gob.es/BoletinOnline2/"
        "sedal/35101000.XLS"
    )
    path_mivau = descargar_archivo(
        url_mivau_general,
        "mivau_valor_tasado_vivienda_libre",
        es_csv=False
    )

    print("\nFiles downloaded to ./data_raw/")
