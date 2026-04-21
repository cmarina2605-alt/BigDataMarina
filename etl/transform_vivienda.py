"""
Transform housing price datasets from EUSTAT (CSV) and MIVAU (XLS)
into a unified annual dataset ready for PostgreSQL ingestion.

Final output columns:
- year
- province
- region_type (Province / CCAA)
- source (Eustat / MIVAU)
- price_per_m2

This script keeps all original heuristics and workarounds required
to process heterogeneous official sources.
"""

from pathlib import Path
import pandas as pd
import re


# ======================================================
# PROJECT STRUCTURE
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_raw"
CLEAN_DIR = BASE_DIR / "data_clean"

CLEAN_DIR.mkdir(exist_ok=True)

# Output file name matches the PostgreSQL table
OUT = CLEAN_DIR / "housing_prices_annual.csv"


# ======================================================
# FILE DISCOVERY
# ======================================================

def find_source_files():
    """
    Locate the most recent EUSTAT CSV and MIVAU XLS files.

    The script assumes that:
    - EUSTAT provides a single CSV file
    - MIVAU may provide multiple historical XLS files
    """
    eustat_files = sorted(DATA_DIR.glob("eustat_precios_vivienda.csv"))
    mivau_files = sorted(DATA_DIR.glob("mivau_valor_tasado_vivienda_libre_*.xls"))

    return (
        eustat_files[-1] if eustat_files else None,
        mivau_files[-1] if mivau_files else None
    )


# ======================================================
# EUSTAT PROCESSING
# ======================================================

def process_eustat(path):
    """
    Process EUSTAT housing price CSV.

    The dataset is quarterly and wide-format.
    This function:
    - Cleans numeric values
    - Extracts province-level prices
    - Aggregates quarterly data into annual averages
    """

    if not path:
        return pd.DataFrame()

    print(f"Processing EUSTAT: {path}")

    try:
        df = pd.read_csv(
            path,
            sep=';',
            header=None,
            skiprows=4,
            engine='python',
            on_bad_lines='skip',
            encoding='utf-8'
        )

        column_names = [
            'trimestre',
            'euskadi_total', 'euskadi_nueva', 'euskadi_usada',
            'alava_total', 'alava_nueva', 'alava_usada',
            'bizkaia_total', 'bizkaia_nueva', 'bizkaia_usada',
            'gipuzkoa_total', 'gipuzkoa_nueva', 'gipuzkoa_usada'
        ]

        if len(df.columns) != len(column_names):
            print("EUSTAT column count does not match expected structure.")
            return pd.DataFrame()

        df.columns = column_names

        # Extract year from the quarter column
        df['year'] = pd.to_numeric(df['trimestre'], errors='coerce').ffill().astype('Int64')
        df = df[df['trimestre'].str.match(r'^[IVX]+$', na=False)]

        # Clean numeric price columns
        price_cols = [c for c in df.columns if c.endswith('_total')]
        for c in price_cols:
            df[c] = pd.to_numeric(
                df[c].astype(str)
                .str.replace(r'\.', '', regex=True)
                .str.replace(',', '.'),
                errors='coerce'
            )

        # Convert to flat structure by province
        df_flat = pd.DataFrame()
        for prov, col in [
            ('Álava', 'alava_total'),
            ('Bizkaia', 'bizkaia_total'),
            ('Gipuzkoa', 'gipuzkoa_total')
        ]:
            tmp = df[['year', col]].copy().rename(columns={col: 'price_per_m2'})
            tmp['province'] = prov
            tmp['region_type'] = 'Province'
            tmp['source'] = 'Eustat'
            df_flat = pd.concat([df_flat, tmp])

        # Annual aggregation
        df_anual = (
            df_flat
            .groupby(['year', 'province', 'region_type', 'source'])['price_per_m2']
            .mean()
            .reset_index()
        )

        df_anual['price_per_m2'] = df_anual['price_per_m2'].round(2)

        return df_anual

    except Exception as e:
        print(f"EUSTAT processing error: {e}")
        return pd.DataFrame()


# ======================================================
# MIVAU PROCESSING
# ======================================================

def process_mivau(path):
    """
    Process MIVAU XLS historical housing price data.

    The structure is highly irregular:
    - Multiple sheets
    - Quarters distributed across columns
    - Province/CCAA mixed values

    This function preserves all original heuristics used
    to extract valid annual averages.
    """

    if not path:
        return pd.DataFrame()

    print(f"Processing MIVAU XLS: {path}")

    try:
        xl = pd.ExcelFile(path, engine='xlrd')
        dfs = []

        for sheet in xl.sheet_names:
            print(f"  Sheet: {sheet}")

            df = pd.read_excel(
                xl,
                sheet_name=sheet,
                skiprows=13,
                header=0,
                engine='xlrd'
            )

            df = df.dropna(how='all').reset_index(drop=True)
            if len(df) < 5:
                continue

            prov_col = df.columns[1]

            # Detect quarter columns
            trim_cols = [
                c for c in df.columns[2:]
                if 'º' in str(c) and df[c].notna().any()
            ]

            if not trim_cols:
                continue

            # Extract years from sheet name
            years = re.findall(r'\d{4}', sheet)
            if not years:
                continue

            year_map = {
                trim: int(years[min(i, len(years)-1)])
                for i, trim in enumerate(trim_cols)
            }

            # Melt to long format
            df_melt = pd.melt(
                df,
                id_vars=[prov_col],
                value_vars=trim_cols,
                var_name='trimestre',
                value_name='price_per_m2'
            )

            df_melt['year'] = df_melt['trimestre'].map(year_map)

            # Normalize province names
            df_melt['province'] = (
                df_melt[prov_col]
                .astype(str)
                .str.strip()
                .str.title()
                .replace({
                    r'Araba/.*': 'Araba/Álava',
                    r'Álava': 'Araba/Álava',
                    r'Vizcaya': 'Bizkaia',
                    r'Guipúzcoa': 'Gipuzkoa',
                    r'Principado de Asturias': 'Asturias',
                    r'Islas Baleares': 'Illes Balears',
                    r'Comunidad Foral de Navarra': 'Navarra',
                    r'Comunidad de Madrid': 'Madrid',
                    r'Región de Murcia': 'Murcia',
                    r'La Rioja': 'La Rioja'
                }, regex=True)
            )

            # Detect province vs CCAA
            df_melt['region_type'] = df_melt['province'].apply(
                lambda x: 'CCAA' if x in [
                    'Andalucía', 'Aragón', 'Asturias', 'Illes Balears',
                    'Canarias', 'Cantabria', 'Castilla y León',
                    'Castilla-La Mancha', 'Cataluña',
                    'Comunidad Valenciana', 'Extremadura',
                    'Galicia', 'Madrid', 'Murcia',
                    'Navarra', 'País Vasco', 'La Rioja'
                ] else 'Province'
            )

            df_melt['source'] = 'MIVAU'

            df_melt['price_per_m2'] = pd.to_numeric(df_melt['price_per_m2'], errors='coerce')
            df_melt = df_melt.dropna(subset=['year', 'price_per_m2'])

            # Annual aggregation
            df_anual = (
                df_melt
                .groupby(['year', 'province', 'region_type', 'source'])['price_per_m2']
                .mean()
                .reset_index()
            )

            df_anual['price_per_m2'] = df_anual['price_per_m2'].round(2)
            dfs.append(df_anual)

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    except Exception as e:
        print(f"MIVAU processing error: {e}")
        return pd.DataFrame()


# ======================================================
# MAIN PIPELINE
# ======================================================

def main():

    print("Full transformation: housing prices (annual)\n")

    e_path, m_path = find_source_files()

    df_eustat = process_eustat(e_path)
    df_mivau = process_mivau(m_path)

    df_final = pd.concat([df_eustat, df_mivau], ignore_index=True)

    if df_final.empty:
        print("No data processed.")
        return

    # Final column order (matches database table)
    df_final = df_final[
        ['year', 'province', 'region_type', 'source', 'price_per_m2']
    ].sort_values(['province', 'year', 'source'])

    df_final.to_csv(OUT, index=False, encoding='utf-8-sig')

    print(f"\nFinal CSV generated: {OUT}")
    print(f"Total rows: {len(df_final)}")
    print(df_final.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
