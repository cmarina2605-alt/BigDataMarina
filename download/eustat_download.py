"""
EUSTAT population data download and transformation script (HTML to CSV).

This script downloads population data from the EUSTAT website in HTML format,
extracts the main data table, transforms it into a clean and normalized
structure, and stores the result as a CSV file.

The script handles HTML tables that may contain multi-level headers and
non-standard numeric formats.
"""

import requests
import pandas as pd
import time
from pathlib import Path
from io import StringIO


# ======================================================
# PROJECT PATHS
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_raw"
DATA_DIR.mkdir(exist_ok=True)

URL = "https://www.eustat.eus/elementos/ele0013900/tbl0013998_c.html"


# ======================================================
# EUSTAT TABLE CLEANING
# ======================================================

def clean_table(df):
    """
    Cleans and reshapes the EUSTAT population table.

    The function performs several transformation steps:
    - Flattens multi-level column headers if present
    - Converts the table from wide to tidy format
    - Separates province and sex information
    - Pivots sex categories into individual columns
    - Cleans numeric values and converts them to numeric types

    Some of these steps may appear redundant, but they are intentionally
    kept to ensure robustness when dealing with HTML tables that may
    change structure over time.
    """

    # --------------------------------------------------
    # 1. Flatten MultiIndex columns if present
    # --------------------------------------------------
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join([str(c) for c in col if c])
            for col in df.columns
        ]

    # The first column represents nationality
    df = df.rename(columns={df.columns[0]: "nacionalidad"})

    # --------------------------------------------------
    # 2. Convert province/sex columns into rows
    # --------------------------------------------------
    tidy = (
        df
        .melt(
            id_vars="nacionalidad",
            var_name="prov_sexo",
            value_name="poblacion"
        )
    )

    # Split combined column into province and sex
    # Example: "Bizkaia_Total" -> "Bizkaia" + "Total"
    tidy[["provincia", "sexo"]] = tidy["prov_sexo"].str.rsplit(
        "_", n=1, expand=True
    )

    tidy = tidy.drop(columns=["prov_sexo"])

    # --------------------------------------------------
    # 3. Pivot sex values into columns
    # --------------------------------------------------
    tidy = (
        tidy
        .pivot_table(
            index=["provincia", "nacionalidad"],
            columns="sexo",
            values="poblacion",
            aggfunc="first"
        )
        .reset_index()
    )

    tidy.columns.name = None

    tidy = tidy.rename(columns={
        "Total": "total",
        "Hombres": "hombres",
        "Mujeres": "mujeres"
    })

    # --------------------------------------------------
    # 4. Clean numeric values
    # --------------------------------------------------
    # EUSTAT uses dot as thousands separator and comma as decimal separator
    for c in ["total", "hombres", "mujeres"]:
        tidy[c] = (
            tidy[c].astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        tidy[c] = pd.to_numeric(tidy[c], errors="coerce")

    return tidy


# ======================================================
# DOWNLOAD AND PARSE
# ======================================================

def run():
    """
    Downloads the EUSTAT HTML page, extracts the population table,
    cleans the data, and stores it as a CSV file.
    """

    print("\n====== EUSTAT DOWNLOAD ======")
    start = time.time()

    r = requests.get(URL, timeout=60)
    r.raise_for_status()

    # Read first table from HTML
    df = pd.read_html(StringIO(r.text))[0]

    # Clean and transform table
    df = clean_table(df)

    output = DATA_DIR / "eustat_population.csv"
    df.to_csv(output, index=False)

    print(f"OK ({round(time.time() - start, 2)}s)")
    print("Saved to:", output)


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
