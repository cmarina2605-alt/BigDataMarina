"""
Basque Parliament election results extraction script.

This script downloads election results from an external HTML webpage,
parses a table containing parliamentary seat distribution, transforms
the data into a normalized structure, and stores the result as a CSV file.

The script combines web scraping and data transformation logic and
assumes that the source website structure remains relatively stable.
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from io import StringIO
import re


# ======================================================
# CONFIGURATION
# ======================================================

URL = "http://www.historiaelectoral.com/aeuzkadi.html"
HEADERS = {"User-Agent": "Mozilla/5.0"}

BASE_DIR = Path(__file__).resolve().parent.parent
OUT = BASE_DIR / "data_clean/elections_clean.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)


# ======================================================
# DOWNLOAD HTML
# ======================================================

print("Downloading election results page...")

try:
    response = requests.get(URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    html = response.text

except requests.exceptions.RequestException as e:
    print("Failed to download election results page.")
    print("Error message:", e)
    raise SystemExit(1)


# ======================================================
# PARSE HTML AND LOCATE TABLE
# ======================================================

soup = BeautifulSoup(html, "lxml")

title_node = soup.find(
    string=lambda t: t and "Grupos parlamentarios" in t
)

if title_node is None:
    print("Expected table title not found in HTML.")
    raise SystemExit(1)

table = title_node.find_next("table")

if table is None:
    print("Election results table not found.")
    raise SystemExit(1)


# ======================================================
# READ TABLE INTO DATAFRAME
# ======================================================

try:
    df = pd.read_html(StringIO(str(table)), header=None)[0]
except ValueError:
    print("Failed to parse HTML table into DataFrame.")
    raise SystemExit(1)


# ======================================================
# EXTRACT MONTH AND YEAR FROM HEADER ROW
# ======================================================
# The first row contains election dates in the format:
# <Month> <Year>

months = []
years = []

for cell in df.iloc[0, 1:]:
    text = str(cell).strip()

    match = re.search(r"(\D+)\s*(\d{4})", text)

    if match:
        month = match.group(1).strip()
        year = match.group(2)
    else:
        month = ""
        year = ""

    months.append(month)
    years.append(year)


# ======================================================
# CLEAN DATAFRAME
# ======================================================

# Remove header row
df = df.iloc[1:].reset_index(drop=True)

# Remove rows without party name
df = df[df.iloc[:, 0].notna()]


# ======================================================
# NORMALIZE DATA (WIDE TO LONG)
# ======================================================

rows = []

for i in range(len(df)):
    party = df.iloc[i, 0]

    for j in range(1, len(df.columns)):
        seats = df.iloc[i, j]

        if pd.notna(seats):
            rows.append([
                party,
                years[j - 1],
                months[j - 1],
                int(seats)
            ])


result = pd.DataFrame(
    rows,
    columns=["party_name", "year", "month", "seats"]
)


# ======================================================
# OUTPUT CSV
# ======================================================

result.to_csv(OUT, index=False, encoding="utf-8-sig")

print("Election results CSV successfully generated:", OUT)
print(result.head())
