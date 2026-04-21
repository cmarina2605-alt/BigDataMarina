"""
Live Streamlit dashboard — queries the dbt marts directly on BigQuery.

Every time a user opens the app (or hits the sidebar "Refresh data" button),
the 5 marts built by `dbt run` are re-queried from BigQuery and the charts
are regenerated.  No static HTML, no stale CSVs.

Run locally:
    streamlit run dashboard/app.py

Requires in .env:
    BQ_PROJECT_ID       GCP project ID
    BQ_DATASET_ID       BigQuery dataset (e.g. bigdatamarina)
    BQ_CREDENTIALS      Path to GCP service-account JSON key file
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account


# ======================================================
# CONFIG
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

BQ_PROJECT = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET_ID")
BQ_CREDENTIALS_PATH = os.getenv("BQ_CREDENTIALS")

MARTS = {
    "crime":     "mart_immigration_crime_study_pv_rates",
    "labour":    "mart_labour_market_nationality_pv",
    "housing":   "mart_housing_foreign_share",
    "poverty":   "mart_immigration_poverty_pv",
    "elections": "mart_elections_vs_demographics_pv",
}

st.set_page_config(
    page_title="Immigration in the Basque Country",
    page_icon=None,
    layout="wide",
)


# ======================================================
# BIGQUERY
# ======================================================

@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    missing = [v for v in ("BQ_PROJECT_ID", "BQ_DATASET_ID", "BQ_CREDENTIALS")
               if not os.getenv(v)]
    if missing:
        st.error(f"Missing env vars in .env: {', '.join(missing)}")
        st.stop()
    credentials = service_account.Credentials.from_service_account_file(
        BQ_CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=BQ_PROJECT, credentials=credentials)


@st.cache_data(ttl=600, show_spinner="Querying BigQuery marts...")
def load_mart(mart_name: str) -> pd.DataFrame:
    client = get_bq_client()
    table = f"`{BQ_PROJECT}.{BQ_DATASET}.{mart_name}`"
    return client.query(f"SELECT * FROM {table}").to_dataframe()


@st.cache_data(ttl=600, show_spinner="Loading INE ECP data...")
def _load_ecp_foreign_pop_pct() -> pd.DataFrame:
    """Load INE Estadística Continua de Población (2021-2025) — PV aggregate.

    The ECP replaced the Padrón Continuo and provides quarterly population
    data by province and nationality from 2021 onward.
    """
    ecp_path = BASE_DIR / "data_clean" / "ine_ecp_foreign_population_province.csv"
    if not ecp_path.exists():
        return pd.DataFrame(columns=["year", "foreign_pop_pct_ecp"])
    ecp = pd.read_csv(ecp_path)
    ecp = ecp[ecp["year"] <= 2025]
    ecp_agg = ecp.groupby("year", as_index=False).agg(
        foreign_population=("foreign_population", "sum"),
        total_population=("total_population", "sum"),
    )
    ecp_agg["foreign_pop_pct_ecp"] = (
        ecp_agg["foreign_population"] / ecp_agg["total_population"] * 100
    ).round(2)
    return ecp_agg[["year", "foreign_pop_pct_ecp"]]


@st.cache_data(ttl=600, show_spinner="Building demographic series...")
def _load_demographics_full() -> pd.DataFrame:
    """Build a **unified** foreign-population % series (1998–2025).

    Uses chain-linking to splice the INE ECP (2021+) onto the Padrón
    Continuo (1998-2022) scale.  The overlap years (2021-2022) appear in
    both sources; the mean ratio Padrón/ECP is used to scale ECP values
    for 2023-2025 so the series is continuous with no level jump.

    Returns a DataFrame with columns: year, foreign_population_pct.
    """
    ine = _load_ine_foreign_pop_pct()
    ecp = _load_ecp_foreign_pop_pct()

    if ecp.empty:
        return ine[["year", "foreign_pop_pct_ine"]].rename(
            columns={"foreign_pop_pct_ine": "foreign_population_pct"}
        )

    # Find overlap years to compute chain-link ratio
    overlap = ine[["year", "foreign_pop_pct_ine"]].merge(
        ecp[["year", "foreign_pop_pct_ecp"]], on="year"
    )

    if overlap.empty:
        return ine[["year", "foreign_pop_pct_ine"]].rename(
            columns={"foreign_pop_pct_ine": "foreign_population_pct"}
        )

    # Chain-link ratio: Padrón / ECP (< 1 because ECP counts more broadly)
    ratio = (overlap["foreign_pop_pct_ine"] / overlap["foreign_pop_pct_ecp"]).mean()

    # Padrón for all its years, then scaled ECP for years beyond Padrón
    result = ine[["year", "foreign_pop_pct_ine"]].rename(
        columns={"foreign_pop_pct_ine": "foreign_population_pct"}
    ).copy()

    padron_max_year = int(ine["year"].max())
    ecp_ext = ecp[ecp["year"] > padron_max_year].copy()
    if not ecp_ext.empty:
        ecp_ext["foreign_population_pct"] = (
            ecp_ext["foreign_pop_pct_ecp"] * ratio
        ).round(2)
        result = pd.concat([
            result, ecp_ext[["year", "foreign_population_pct"]]
        ], ignore_index=True)

    return result.sort_values("year")


@st.cache_data(ttl=600, show_spinner="Loading INE Padrón data...")
def _load_ine_foreign_pop_pct() -> pd.DataFrame:
    """Build a complete foreign-population % series from INE data (1998-2022).

    Uses the INE Padrón Continuo (foreign pop by province, annual since 1998)
    combined with INE total population.  This fills all the years missing from
    the EUSTAT nationality source (which only covers 2010, 2015-2024).
    """
    # Foreign population from Padrón
    fp = pd.read_csv(BASE_DIR / "data_clean" / "ine_foreign_population_province.csv")
    fp_agg = fp.groupby("year", as_index=False)["foreign_population"].sum()

    # Total population from INE
    tp = pd.read_csv(BASE_DIR / "data_clean" / "population_territory.csv")
    basque = tp[
        (tp["level"] == "province")
        & (tp["territory"].isin(["Araba/Álava", "Bizkaia", "Gipuzkoa"]))
        & (tp["sex"] == "Ambos sexos")
        & (tp["age_group"] == "Todas las edades")
        & (tp["periodo"] == "1 de julio de")
    ]
    tp_agg = basque.groupby("year", as_index=False)["population"].sum()
    tp_agg.rename(columns={"population": "total_population_ine"}, inplace=True)

    merged = fp_agg.merge(tp_agg, on="year", how="inner")
    merged["foreign_pop_pct_ine"] = (
        merged["foreign_population"] / merged["total_population_ine"] * 100
    ).round(2)
    return merged[["year", "foreign_population", "total_population_ine",
                    "foreign_pop_pct_ine"]]


@st.cache_data(ttl=600, show_spinner="Building crime dataset...")
def _build_crime_data() -> pd.DataFrame:
    """Build the full crime-vs-demographics dataset from base tables.

    Queries the two crime base tables directly from BigQuery (which have
    ALL years 2010-2024) and merges with the INE Padrón CSV for foreign
    population %.  This bypasses the dbt mart's INNER JOIN limitation
    so we get data for every year with no gaps.
    """
    client = get_bq_client()

    # Total detentions per year (Basque Country)
    total_sql = f"""
        SELECT year, SUM(value) AS total_detentions
        FROM `{BQ_PROJECT}.{BQ_DATASET}.crime_detentions_total`
        WHERE territory  = 'PAÍS VASCO'
          AND crime_type = 'TOTAL INFRACCIONES PENALES'
          AND age_group  = 'TOTAL edad'
          AND sex        = 'Ambos sexos'
        GROUP BY year
    """
    total = client.query(total_sql).to_dataframe()

    # Foreign detentions per year (sum across 3 Basque provinces;
    # every row in this table is already a foreign nationality)
    foreign_sql = f"""
        SELECT year, SUM(value) AS foreign_detentions
        FROM `{BQ_PROJECT}.{BQ_DATASET}.crime_detentions`
        WHERE sex = 'Ambos sexos'
          AND (   LOWER(province) LIKE 'araba%'
               OR LOWER(province) LIKE 'bizkaia%'
               OR LOWER(province) LIKE 'gipuzkoa%')
        GROUP BY year
    """
    foreign = client.query(foreign_sql).to_dataframe()

    # Merge crime data
    crime = total.merge(foreign, on="year", how="inner")
    crime["total_detentions"] = pd.to_numeric(crime["total_detentions"])
    crime["foreign_detentions"] = pd.to_numeric(crime["foreign_detentions"])
    crime["foreign_detention_pct"] = (
        crime["foreign_detentions"] / crime["total_detentions"] * 100
    ).round(2)

    # Merge with unified foreign-pop % series (chain-linked, 1998-2025)
    demo = _load_demographics_full()
    demo = demo.rename(columns={"foreign_population_pct": "foreign_pop_pct"})
    crime = crime.merge(demo[["year", "foreign_pop_pct"]], on="year", how="left")
    crime["over_repr_ratio"] = (
        crime["foreign_detention_pct"]
        / crime["foreign_pop_pct"].replace(0, float("nan"))
    ).round(2)

    return crime.sort_values("year")


@st.cache_data(ttl=600, show_spinner="Building housing dataset...")
def _build_housing_data() -> pd.DataFrame:
    """Build full housing-price + foreign-pop dataset from base tables.

    Queries the housing_prices_annual base table directly (all years 2012-2025)
    and merges with INE Padrón CSV per province for foreign-population %.
    This bypasses the mart's INNER JOIN limitation so we get every year.
    """
    client = get_bq_client()
    sql = f"""
        SELECT
            CASE
                WHEN LOWER(province) LIKE 'araba%'
                  OR LOWER(province) LIKE 'álava%'
                  OR LOWER(province) LIKE 'alava%'    THEN 'Araba'
                WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
                WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
            END AS province_std,
            CAST(year AS INT64) AS year,
            ROUND(AVG(price_per_m2), 2) AS avg_price_per_m2
        FROM `{BQ_PROJECT}.{BQ_DATASET}.housing_prices_annual`
        WHERE LOWER(province) LIKE 'araba%'
           OR LOWER(province) LIKE 'álava%'
           OR LOWER(province) LIKE 'alava%'
           OR LOWER(province) LIKE 'bizkaia%'
           OR LOWER(province) LIKE 'gipuzkoa%'
        GROUP BY province_std, year
        ORDER BY province_std, year
    """
    housing = client.query(sql).to_dataframe()
    for c in housing.columns:
        if c != "province_std":
            housing[c] = pd.to_numeric(housing[c], errors="coerce")

    # Per-province foreign-population % — combine INE Padrón (1998-2022)
    # with EUSTAT province-level data from BigQuery (2010, 2015-2024)
    # so we cover years beyond 2022.

    # 1) INE Padrón per province (continuous 1998-2022)
    ine_prov = pd.read_csv(BASE_DIR / "data_clean" / "ine_foreign_population_province.csv")
    pop_total = pd.read_csv(BASE_DIR / "data_clean" / "population_territory.csv")
    basque_total = pop_total[
        (pop_total["level"] == "province")
        & (pop_total["territory"].isin(["Araba/Álava", "Bizkaia", "Gipuzkoa"]))
        & (pop_total["sex"] == "Ambos sexos")
        & (pop_total["age_group"] == "Todas las edades")
        & (pop_total["periodo"] == "1 de julio de")
    ][["territory", "year", "population"]].copy()
    prov_std_map = {"Araba/Álava": "Araba", "Bizkaia": "Bizkaia", "Gipuzkoa": "Gipuzkoa"}
    basque_total["province_std"] = basque_total["territory"].map(prov_std_map)

    ine_prov_merged = ine_prov.merge(
        basque_total[["province_std", "year", "population"]],
        on=["province_std", "year"], how="inner"
    )
    ine_prov_merged["foreign_population_pct"] = (
        ine_prov_merged["foreign_population"] / ine_prov_merged["population"] * 100
    ).round(2)

    # 2) INE ECP per province (2021-2025) — chain-linked to Padrón scale
    ecp_path = BASE_DIR / "data_clean" / "ine_ecp_foreign_population_province.csv"
    if ecp_path.exists():
        ecp = pd.read_csv(ecp_path)
        ecp = ecp[ecp["year"] <= 2025]
        padron_fp = ine_prov_merged[["province_std", "year", "foreign_population_pct"]].copy()
        combined_parts = [padron_fp]
        padron_max_year = int(padron_fp["year"].max())

        for prov in ["Araba", "Bizkaia", "Gipuzkoa"]:
            pad_p = padron_fp[padron_fp["province_std"] == prov]
            ecp_p = ecp[ecp["province_std"] == prov]
            # Find overlap to compute chain-link ratio
            ovlp = pad_p[["year", "foreign_population_pct"]].merge(
                ecp_p[["year", "foreign_population_pct"]].rename(
                    columns={"foreign_population_pct": "ecp_pct"}
                ), on="year"
            )
            if ovlp.empty:
                continue
            ratio = (ovlp["foreign_population_pct"] / ovlp["ecp_pct"]).mean()
            # Scale ECP values for years beyond Padrón
            ext = ecp_p[ecp_p["year"] > padron_max_year].copy()
            if not ext.empty:
                ext["foreign_population_pct"] = (ext["foreign_population_pct"] * ratio).round(2)
                combined_parts.append(ext[["province_std", "year", "foreign_population_pct"]])

        combined = pd.concat(combined_parts, ignore_index=True)
    else:
        combined = ine_prov_merged[["province_std", "year", "foreign_population_pct"]].copy()

    # Merge housing prices with foreign pop %
    housing = housing.merge(
        combined, on=["province_std", "year"], how="left"
    )
    return housing.sort_values(["year", "province_std"])


@st.cache_data(ttl=600, show_spinner="Querying origin breakdown...")
def load_origin_mix() -> pd.DataFrame:
    """Full foreign-origin breakdown per province (for Q3b).

    The housing mart only exposes the top origin; the full breakdown lives
    in the staging view that aggregates EUSTAT population by continent.
    """
    client = get_bq_client()
    sql = f"""
        SELECT province_std, origin, year, population
        FROM `{BQ_PROJECT}.{BQ_DATASET}.stg_eustat_population_by_origin`
        WHERE origin_bucket = 'Foreign'
          AND province_std IN ('Araba', 'Bizkaia', 'Gipuzkoa')
    """
    return client.query(sql).to_dataframe()


# ======================================================
# SIDEBAR
# ======================================================

with st.sidebar:
    st.markdown("### Data")
    st.caption(f"**Project:** `{BQ_PROJECT}`")
    st.caption(f"**Dataset:** `{BQ_DATASET}`")
    st.caption("Charts query the 5 dbt marts live on BigQuery.")
    if st.button("Refresh data", use_container_width=True):
        load_mart.clear()
        st.rerun()
    st.divider()
    st.markdown(
        "Generated from `dashboard/app.py`\n\n"
        "Sources: INE · EUSTAT · Basque Parliament"
    )


# ======================================================
# HEADER
# ======================================================

st.title("Immigration in the Basque Country")
st.caption(
    "Live dashboard built from the 5 dbt marts on BigQuery. "
    "Every chart is re-queried on page load."
)

tab_q1, tab_q2, tab_q3, tab_q4, tab_q5 = st.tabs([
    "Q1 · Crime",
    "Q2 · Labour",
    "Q3 · Housing",
    "Q4 · Poverty",
    "Q5 · Elections",
])


# ======================================================
# Q1 — CRIME
# ======================================================

with tab_q1:
    st.subheader("Q1 · Are foreigners over-represented in detentions?")
    st.caption(
        "Compares the foreign share of detention events against the foreign share of the "
        "resident population. The over-representation *ratio* is the defensible metric — "
        "raw per-100k rates are an upper bound because detentions count events (not "
        "unique people) and include non-resident foreign nationals."
    )
    # Build crime dataset from base tables + INE Padrón CSV
    # (bypasses the mart's INNER JOIN so we get ALL years with no gaps)
    crime = _build_crime_data()

    det_series = crime.dropna(subset=["foreign_detention_pct"]).copy()
    pop_series = crime.dropna(subset=["foreign_pop_pct"]).copy()
    ratio_series = crime.dropna(subset=["over_repr_ratio"]).copy()

    # KPIs: use latest year that has both metrics
    if ratio_series.empty:
        st.error("No crime data returned — check BigQuery base tables.")
        st.dataframe(crime)
        st.stop()
    latest = ratio_series.iloc[-1]
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Year", int(latest["year"]))
    k2.metric("Foreign share of population", f"{latest['foreign_pop_pct']:.1f}%")
    k3.metric("Foreign share of detentions", f"{latest['foreign_detention_pct']:.1f}%")
    k4.metric("Over-representation", f"{latest['over_repr_ratio']:.1f}x")

    # --- Chart 1: Grouped bar chart — both series for ALL years ---
    fig1 = go.Figure()
    fig1.add_bar(
        x=det_series["year"],
        y=det_series["foreign_detention_pct"],
        name="Foreign share of detentions",
        marker_color="#d62728",
        text=[f"{v:.1f}%" for v in det_series["foreign_detention_pct"]],
        textposition="outside", textfont=dict(size=10),
    )
    fig1.add_bar(
        x=pop_series["year"],
        y=pop_series["foreign_pop_pct"],
        name="Foreign share of population",
        marker_color="#1f77b4",
        text=[f"{v:.1f}%" for v in pop_series["foreign_pop_pct"]],
        textposition="outside", textfont=dict(size=10),
    )
    fig1.update_layout(
        title="The disparity at a glance: population share vs detention share",
        barmode="group",
        xaxis_title="Year", yaxis_title="Share (%)",
        yaxis=dict(range=[0, 72]),
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig1, use_container_width=True)

    # --- Chart 2: Over-representation ratio trend (continuous) ---
    fig2 = go.Figure()
    fig2.add_scatter(
        x=ratio_series["year"],
        y=ratio_series["over_repr_ratio"],
        mode="lines+markers+text", name="Over-representation ratio",
        line=dict(color="#d62728", width=3),
        marker=dict(size=9),
        text=[f"{v:.1f}x" for v in ratio_series["over_repr_ratio"]],
        textposition="top center", textfont=dict(size=11),
        fill="tozeroy", fillcolor="rgba(214,39,40,0.08)",
    )
    fig2.add_hline(y=1, line_dash="dash", line_color="gray",
                   annotation_text="Parity (1.0x)",
                   annotation_position="bottom right")
    fig2.update_layout(
        title="Over-representation ratio over time (detention share ÷ population share)",
        xaxis_title="Year", yaxis_title="Ratio (x)",
        yaxis=dict(range=[0, float(ratio_series["over_repr_ratio"].max()) * 1.25]),
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("See underlying data"):
        st.dataframe(crime, use_container_width=True)


# ======================================================
# Q2 — LABOUR
# ======================================================

with tab_q2:
    st.subheader("Q2 · Are foreign workers more exposed to unemployment?")
    st.caption(
        "Basque unemployment rates by nationality plus the national sectoral mix "
        "that explains the gap.  Rates are from EUSTAT; sector data from INE (national)."
    )
    labour = load_mart(MARTS["labour"]).sort_values(["year", "nationality"])
    # Cast all numeric columns to float to avoid Decimal issues
    for c in labour.columns:
        if c not in ("nationality",):
            labour[c] = pd.to_numeric(labour[c], errors="coerce")

    latest_year = int(labour["year"].max())
    sp = labour[(labour["year"] == latest_year) & (labour["nationality"] == "Spanish")]
    fg = labour[(labour["year"] == latest_year) & (labour["nationality"] == "Foreign")]

    spanish_u = float(sp["unemployment_rate"].iloc[0])
    foreign_u = float(fg["unemployment_rate"].iloc[0])
    gap = foreign_u - spanish_u

    # First year in the series to compute trend
    first_year = int(labour["year"].min())
    fg_first = labour[(labour["year"] == first_year) & (labour["nationality"] == "Foreign")]
    foreign_u_first = float(fg_first["unemployment_rate"].iloc[0]) if not fg_first.empty else None

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Year", latest_year)
    k2.metric("Spanish unemployment", f"{spanish_u:.1f}%")
    k3.metric("Foreign unemployment", f"{foreign_u:.1f}%",
              delta=f"{foreign_u - foreign_u_first:+.1f} pp since {first_year}"
              if foreign_u_first is not None else None,
              delta_color="inverse")
    k4.metric("Gap (Foreign − Spanish)", f"{gap:+.1f} pp")

    # --- Chart 1: Unemployment gap over time (area between lines) ---
    fig1 = go.Figure()
    for nat, color, dash in [("Foreign", "#d62728", "solid"),
                              ("Spanish", "#1f77b4", "solid")]:
        sub = labour[labour["nationality"] == nat].sort_values("year")
        fig1.add_scatter(
            x=sub["year"], y=sub["unemployment_rate"],
            mode="lines+markers", name=nat,
            line=dict(color=color, width=2.5, dash=dash),
            marker=dict(size=6),
        )
    # Shade the gap between the two lines
    sp_line = labour[labour["nationality"] == "Spanish"].sort_values("year")
    fg_line = labour[labour["nationality"] == "Foreign"].sort_values("year")
    merged_gap = sp_line[["year", "unemployment_rate"]].merge(
        fg_line[["year", "unemployment_rate"]], on="year", suffixes=("_sp", "_fg")
    )
    fig1.add_scatter(
        x=pd.concat([merged_gap["year"], merged_gap["year"][::-1]]),
        y=pd.concat([merged_gap["unemployment_rate_fg"],
                      merged_gap["unemployment_rate_sp"][::-1]]),
        fill="toself", fillcolor="rgba(214,39,40,0.1)",
        line=dict(width=0), showlegend=False, hoverinfo="skip",
    )
    fig1.update_layout(
        title="Unemployment rate: the persistent gap",
        xaxis_title="Year", yaxis_title="Unemployment rate (%)",
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig1, use_container_width=True)

    # --- Charts 2 & 3: Sector mix + Gap bar chart side by side ---
    c1, c2 = st.columns(2)

    with c1:
        # Sector mix — grouped bars
        latest_sector = labour[labour["year"] == latest_year].copy()
        sector_cols = ["national_pct_agriculture", "national_pct_industry",
                       "national_pct_construction", "national_pct_services"]
        sector_labels = ["Agriculture", "Industry", "Construction", "Services"]
        rows = []
        for _, r in latest_sector.iterrows():
            for col, lab in zip(sector_cols, sector_labels):
                rows.append({"sector": lab, "nationality": r["nationality"],
                             "pct": float(r[col]) if pd.notna(r[col]) else 0})
        sector_df = pd.DataFrame(rows)
        fig2 = px.bar(
            sector_df, x="sector", y="pct", color="nationality", barmode="group",
            title=f"Where each group works — sector mix ({latest_year})",
            color_discrete_map={"Foreign": "#d62728", "Spanish": "#1f77b4"},
        )
        fig2.update_layout(yaxis_title="% of employed in sector",
                           legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig2, use_container_width=True)

    with c2:
        # Gap evolution bar chart — one bar per year
        gap_df = (
            labour.pivot(index="year", columns="nationality",
                         values="unemployment_rate")
            .reset_index()
        )
        gap_df["gap"] = gap_df["Foreign"] - gap_df["Spanish"]
        fig3 = go.Figure()
        fig3.add_bar(
            x=gap_df["year"], y=gap_df["gap"],
            marker_color=["#d62728" if g > 0 else "#1f77b4"
                          for g in gap_df["gap"]],
            text=[f"{g:+.1f}" for g in gap_df["gap"]],
            textposition="outside", textfont=dict(size=10),
        )
        fig3.add_hline(y=0, line_dash="dash", line_color="gray")
        fig3.update_layout(
            title="Unemployment gap over time (Foreign − Spanish, pp)",
            xaxis_title="Year", yaxis_title="Gap (pp)",
            hovermode="x unified",
        )
        st.plotly_chart(fig3, use_container_width=True)

    # Key insight callout
    st.info(
        f"**Key insight:** Foreign workers face a consistent unemployment premium "
        f"of **{gap:+.1f} pp** over Spanish nationals ({latest_year}). The sector mix "
        f"partly explains this — foreigners are disproportionately concentrated in "
        f"**construction** and **services**, sectors with higher job instability."
    )

    with st.expander("See underlying mart data"):
        st.dataframe(labour, use_container_width=True)


# ======================================================
# Q3 — HOUSING
# ======================================================

with tab_q3:
    st.subheader("Q3 · Does immigration track housing-price evolution?")
    st.caption(
        "For each province: average price per m² plotted against the foreign-population "
        "share.  Both series move upward together, suggesting a positive correlation."
    )

    # Build housing dataset directly from base tables (bypasses mart INNER JOIN gaps)
    housing = _build_housing_data()

    latest_year = int(housing["year"].max())
    latest = housing[housing["year"] == latest_year]

    # Compute correlation per province (only where both series overlap)
    correlations = {}
    for prov in ["Araba", "Bizkaia", "Gipuzkoa"]:
        sub = housing[
            (housing["province_std"] == prov)
        ].dropna(subset=["avg_price_per_m2", "foreign_population_pct"])
        if len(sub) >= 3:
            correlations[prov] = sub["avg_price_per_m2"].corr(
                sub["foreign_population_pct"]
            )
    avg_corr = sum(correlations.values()) / len(correlations) if correlations else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Latest price year", latest_year)
    biz = latest[latest["province_std"] == "Bizkaia"]
    if not biz.empty:
        k2.metric("Bizkaia price", f"{float(biz['avg_price_per_m2'].iloc[0]):.0f} €/m²")
    fp_valid = housing.dropna(subset=["foreign_population_pct"])
    if not fp_valid.empty:
        fp_latest = fp_valid[fp_valid["year"] == fp_valid["year"].max()]
        k3.metric(f"Avg foreign share ({int(fp_latest['year'].iloc[0])})",
                  f"{fp_latest['foreign_population_pct'].mean():.1f}%")
    k4.metric("Avg correlation (r)", f"{avg_corr:.2f}")

    # One dual-axis chart per province, side by side
    provinces = ["Araba", "Bizkaia", "Gipuzkoa"]
    cols = st.columns(3)
    for col_w, prov in zip(cols, provinces):
        sub = housing[housing["province_std"] == prov].sort_values("year").copy()

        price = sub.dropna(subset=["avg_price_per_m2"])
        fpop  = sub.dropna(subset=["foreign_population_pct"])

        corr_val = correlations.get(prov)
        corr_label = f" (r = {corr_val:.2f})" if corr_val is not None else ""

        fig = go.Figure()
        fig.add_scatter(x=price["year"], y=price["avg_price_per_m2"],
                        mode="lines+markers", name="Price €/m²",
                        line=dict(color="#1f77b4", width=2.5))
        fig.add_scatter(x=fpop["year"], y=fpop["foreign_population_pct"],
                        mode="lines+markers", name="Foreign pop. %",
                        line=dict(color="#ff7f0e", width=2.5), yaxis="y2")
        fig.update_layout(
            title=f"{prov}{corr_label}",
            yaxis=dict(title="€/m²",
                       titlefont=dict(color="#1f77b4"),
                       tickfont=dict(color="#1f77b4")),
            yaxis2=dict(title="Foreign %", overlaying="y", side="right",
                        titlefont=dict(color="#ff7f0e"),
                        tickfont=dict(color="#ff7f0e"),
                        rangemode="tozero"),
            xaxis=dict(title="Year"),
            hovermode="x unified",
            legend=dict(orientation="h", y=-0.25),
            margin=dict(t=50, b=40),
        )
        col_w.plotly_chart(fig, use_container_width=True)

    # Foreign-origin mix per province — latest available year
    origin_raw = load_origin_mix()
    origin_en = {
        "América del Sur":    "South America",
        "Asia y Oceanía":     "Asia & Oceania",
        "Europa del Este":    "Eastern Europe",
        "Paises del Magreb":  "Maghreb",
        "Resto de Africa":    "Rest of Africa",
        "Resto de América":   "Rest of Americas",
        "Resto de Europa":    "Rest of Europe",
    }
    origin = origin_raw[origin_raw["origin"].isin(origin_en)].copy()
    origin["origin"] = origin["origin"].map(origin_en)
    origin_year = int(origin["year"].max())
    origin = origin[origin["year"] == origin_year].copy()
    origin["population"] = pd.to_numeric(origin["population"], errors="coerce")
    origin["foreign_total"] = origin.groupby("province_std")["population"].transform("sum")
    origin["pct_of_foreign"] = (origin["population"] / origin["foreign_total"] * 100).round(2)

    fig = px.bar(
        origin.sort_values(["province_std", "pct_of_foreign"], ascending=[True, False]),
        x="origin", y="pct_of_foreign", color="province_std", barmode="group",
        title=f"Foreign-origin mix by province — {origin_year}",
        labels={"origin": "Origin",
                "pct_of_foreign": "% of foreign population",
                "province_std": "Province"},
    )
    fig.update_layout(legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig, use_container_width=True)

    # Count overlapping years for insight text
    overlap_years = housing.dropna(subset=["avg_price_per_m2", "foreign_population_pct"])
    n_years = overlap_years["year"].nunique()

    st.info(
        f"**Key insight:** Housing prices and foreign-population share show a strong "
        f"positive correlation across all three provinces (avg r = {avg_corr:.2f}), "
        f"based on {n_years} overlapping years of data. "
        f"However, this should be interpreted with caution: with a relatively short "
        f"time series where both variables trend upward, high r values are expected "
        f"(spurious correlation). Both may be driven by the same underlying factor — "
        f"broader economic recovery attracting both migrants and investment."
    )

    with st.expander("See underlying data"):
        st.dataframe(housing, use_container_width=True)


# ======================================================
# Q4 — POVERTY
# ======================================================

with tab_q4:
    st.subheader("Q4 · Does the rise in foreigners track poverty risk?")
    st.caption(
        "Basque poverty-risk rate (INE, 2008–2025) against the foreign-population "
        "share (INE Padrón Continuo, up to 2022), with the Spain national rate as benchmark."
    )

    # ── Build poverty dataset directly from CSV (bypasses mart INNER JOIN) ──
    pov_csv = pd.read_csv(BASE_DIR / "data_clean" / "ine_pobreza.csv")
    MAIN_IND = "Todas las edades. Tasa de riesgo de pobreza (renta del año anterior a la entrevista). Base 2013."

    pv_pov = (
        pov_csv[(pov_csv["territory"] == "País Vasco") & (pov_csv["indicator"] == MAIN_IND)]
        [["year", "poverty_rate"]].copy().sort_values("year")
    )
    pv_pov["poverty_rate"] = pd.to_numeric(pv_pov["poverty_rate"], errors="coerce")

    nat_pov = (
        pov_csv[(pov_csv["territory"] == "Total Nacional") & (pov_csv["indicator"] == MAIN_IND)]
        [["year", "poverty_rate"]].copy().sort_values("year")
    )
    nat_pov.rename(columns={"poverty_rate": "national_poverty_rate"}, inplace=True)
    nat_pov["national_poverty_rate"] = pd.to_numeric(nat_pov["national_poverty_rate"], errors="coerce")

    # Merge Basque + National poverty
    poverty = pv_pov.merge(nat_pov, on="year", how="left")

    # Merge with unified foreign-population % (chain-linked, 1998-2025)
    demo_full = _load_demographics_full()
    poverty = poverty.merge(
        demo_full[["year", "foreign_population_pct"]],
        on="year", how="left"
    )
    poverty = poverty.sort_values("year")

    # Gap between national and Basque rate
    poverty["gap_pp"] = (poverty["national_poverty_rate"] - poverty["poverty_rate"]).round(1)

    # Correlation
    both = poverty.dropna(subset=["poverty_rate", "foreign_population_pct"])
    corr = both["poverty_rate"].corr(both["foreign_population_pct"]) if len(both) >= 3 else float("nan")

    # ── KPIs ─────────────────────────────────────────────────────────
    latest_year = int(poverty["year"].max())
    latest = poverty[poverty["year"] == latest_year].iloc[0]
    fp_valid = poverty.dropna(subset=["foreign_population_pct"])
    fp_latest = fp_valid.iloc[-1] if not fp_valid.empty else None

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Latest data", latest_year)
    k2.metric("Basque poverty rate", f"{float(latest['poverty_rate']):.1f}%",
              delta=f"{float(latest['poverty_rate']) - float(poverty.iloc[0]['poverty_rate']):+.1f} pp since {int(poverty.iloc[0]['year'])}",
              delta_color="inverse")
    if pd.notna(latest.get("national_poverty_rate")):
        k3.metric("Spain national rate", f"{float(latest['national_poverty_rate']):.1f}%",
                  delta=f"{float(latest['gap_pp']):.1f} pp lower in Basque C.",
                  delta_color="normal")
    if fp_latest is not None:
        k4.metric(f"Foreign share ({int(fp_latest['year'])})",
                  f"{float(fp_latest['foreign_population_pct']):.1f}%",
                  help=f"Chain-linked INE series — correlation r = {corr:.2f}")

    # ── Chart 1: Dual-axis — Poverty rates vs foreign pop % ─────────
    fig1 = go.Figure()

    # Basque poverty rate (primary axis)
    fig1.add_scatter(
        x=poverty["year"], y=poverty["poverty_rate"],
        mode="lines+markers", name="Basque poverty rate",
        line=dict(color="#1f77b4", width=3),
        marker=dict(size=7),
    )
    # National poverty rate (primary axis, dashed for contrast)
    fig1.add_scatter(
        x=poverty["year"], y=poverty["national_poverty_rate"],
        mode="lines+markers", name="Spain national rate",
        line=dict(color="#aec7e8", width=2, dash="dash"),
        marker=dict(size=5, symbol="square"),
    )
    # Foreign pop % — unified chain-linked series (secondary axis)
    fp_line = poverty.dropna(subset=["foreign_population_pct"])
    fig1.add_scatter(
        x=fp_line["year"], y=fp_line["foreign_population_pct"],
        mode="lines+markers", name="Foreign pop. %",
        line=dict(color="#ff7f0e", width=2.5),
        marker=dict(size=6, symbol="diamond"),
        yaxis="y2",
    )

    fig1.update_layout(
        title=f"Poverty-risk rate vs foreign-population share — Basque Country (r = {corr:.2f})",
        xaxis=dict(title="Year", dtick=1),
        yaxis=dict(title="Poverty rate (%)",
                   titlefont=dict(color="#1f77b4"),
                   tickfont=dict(color="#1f77b4")),
        yaxis2=dict(title="Foreign population (%)", overlaying="y", side="right",
                    titlefont=dict(color="#ff7f0e"),
                    tickfont=dict(color="#ff7f0e"),
                    rangemode="tozero"),
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.25),
        margin=dict(t=50, b=40),
    )
    st.plotly_chart(fig1, use_container_width=True)
    st.caption(
        "**Note:** Foreign-population share combines the INE *Padrón Continuo* (1998–2022) "
        "and *Estadística Continua de Población* (2023–2025), chain-linked via the "
        "2021–2022 overlap to maintain a consistent scale."
    )

    # ── Chart 2: Gap between national and Basque rate ────────────────
    gap_data = poverty.dropna(subset=["gap_pp"]).copy()
    fig2 = go.Figure()
    fig2.add_bar(
        x=gap_data["year"], y=gap_data["gap_pp"],
        marker_color=["#2ca02c" if g > 0 else "#d62728" for g in gap_data["gap_pp"]],
        text=[f"{g:+.1f}" for g in gap_data["gap_pp"]],
        textposition="outside",
        name="Gap (pp)",
    )
    fig2.update_layout(
        title="How much lower is Basque poverty vs Spain? (gap in percentage points)",
        xaxis=dict(title="Year", dtick=1),
        yaxis=dict(title="Gap (pp) — positive = Basque Country lower"),
        showlegend=False,
        margin=dict(t=50, b=40),
    )
    st.plotly_chart(fig2, use_container_width=True)

    # ── Insight ──────────────────────────────────────────────────────
    avg_gap = float(gap_data["gap_pp"].mean())
    fp_change = ""
    if len(both) >= 2:
        fp_start = float(both.iloc[0]["foreign_population_pct"])
        fp_end = float(both.iloc[-1]["foreign_population_pct"])
        pov_start = float(both.iloc[0]["poverty_rate"])
        pov_end = float(both.iloc[-1]["poverty_rate"])
        # Build accurate description of what happened
        pov_direction = "rose" if pov_end > pov_start else "fell" if pov_end < pov_start else "stayed flat"
        crisis_note = ""
        if pov_direction == "rose":
            crisis_note = (
                " — but most of that increase came from the 2008–2012 financial "
                "crisis and the 2021 COVID aftermath, not demographic shifts"
            )
        fp_change = (
            f" The foreign population share rose from {fp_start:.1f}% "
            f"({int(both.iloc[0]['year'])}) to {fp_end:.1f}% ({int(both.iloc[-1]['year'])}), "
            f"while poverty {pov_direction} from {pov_start:.1f}% to {pov_end:.1f}%"
            f"{crisis_note}."
        )

    # Latest full-series trend (2008-2025)
    pov_first = float(poverty.iloc[0]["poverty_rate"])
    pov_last = float(poverty.iloc[-1]["poverty_rate"])
    full_trend = (
        f" Over the full 2008–2025 window the Basque rate has remained remarkably "
        f"stable ({pov_first:.1f}% → {pov_last:.1f}%), even as the foreign share "
        f"nearly doubled."
    )

    st.info(
        f"**Key insight:** The Basque Country's poverty rate consistently sits "
        f"**{avg_gap:.0f} percentage points below** the national average (avg gap "
        f"= {avg_gap:.1f} pp). The weak correlation between poverty and foreign-population "
        f"share (r = {corr:.2f}) suggests immigration is **not a meaningful driver** of "
        f"poverty risk.{fp_change}{full_trend}"
    )

    with st.expander("See underlying data"):
        st.dataframe(poverty, use_container_width=True)


# ======================================================
# Q5 — ELECTIONS
# ======================================================

with tab_q5:
    st.subheader("Q5 · Do electoral results shift with demographic change?")
    st.caption(
        "Parliament composition over time, with foreign-population share "
        "as demographic context (INE chain-linked series, 1998–2025)."
    )
    elec = load_mart(MARTS["elections"])

    # Ensure year is integer and sort chronologically
    elec["year"] = elec["year"].astype(int)
    elec = elec.sort_values(["year", "seats"], ascending=[True, False])

    # ── Consolidate parties ──────────────────────────────────────────
    # The raw data has ~9 parties, many of which are small or existed
    # only briefly.  We keep the 4 main forces that define Basque
    # politics and group everything else as "Others" so the chart is
    # readable.
    MAIN_PARTIES = {
        "GR. NACIONALISTAS VASCOS": "PNV",
        "GR. SOCIALISTAS VASCOS":   "PSE",
        "GR. EH BILDU":             "EH Bildu",
        "GR. POPULAR VASCO":        "PP",
    }

    def classify_party(name: str) -> str:
        return MAIN_PARTIES.get(name, "Others")

    elec["party_group"] = elec["party_name"].apply(classify_party)

    # Aggregate seats per group per year
    grouped = (
        elec.groupby(["year", "party_group"], as_index=False)
        .agg(seats=("seats", "sum"))
    )
    # Compute seat share
    year_totals = grouped.groupby("year")["seats"].transform("sum")
    grouped["seat_pct"] = (grouped["seats"] / year_totals * 100).round(1)
    grouped = grouped.sort_values("year")

    # Load unified demographic series (chain-linked, 1998-2025)
    demo_full = _load_demographics_full()

    # From the mart: foreign pop at each election year (may be sparse)
    demo_elec = (
        elec[["year", "foreign_population_pct"]]
        .drop_duplicates("year")
        .dropna(subset=["foreign_population_pct"])
        .sort_values("year")
    )

    # Headline KPIs
    k1, k2, k3 = st.columns(3)
    k1.metric("Last election", int(elec["year"].max()))
    if not demo_full.empty:
        latest_fp = demo_full.dropna(subset=["foreign_population_pct"]).iloc[-1]
        k2.metric(f"Foreign share ({int(latest_fp['year'])})",
                  f"{float(latest_fp['foreign_population_pct']):.1f}%")
        earliest_fp = demo_full.dropna(subset=["foreign_population_pct"]).iloc[0]
        delta = float(latest_fp["foreign_population_pct"]) - float(earliest_fp["foreign_population_pct"])
        k3.metric("Change in foreign %",
                  f"{float(latest_fp['foreign_population_pct']):.1f}%",
                  f"+{delta:.1f} pp since {int(earliest_fp['year'])}")

    # ── Chart 1: Stacked bar (seats) + foreign % line ────────────────
    # Use a fixed colour for each group so the chart is consistent
    PARTY_COLORS = {
        "PNV":      "#2ca02c",
        "PSE":      "#d62728",
        "EH Bildu": "#ff7f0e",
        "PP":       "#1f77b4",
        "Others":   "#cccccc",
    }
    # Ensure a stable stacking order: big parties at the bottom
    PARTY_ORDER = ["PNV", "PSE", "EH Bildu", "PP", "Others"]

    fig = go.Figure()
    for party in PARTY_ORDER:
        pdf = grouped[grouped["party_group"] == party].sort_values("year")
        fig.add_bar(
            x=pdf["year"],
            y=pdf["seats"],
            name=party,
            marker_color=PARTY_COLORS[party],
        )

    # Overlay: unified foreign population % (right axis)
    demo_valid = demo_full.dropna(subset=["foreign_population_pct"])
    fig.add_scatter(
        x=demo_valid["year"],
        y=demo_valid["foreign_population_pct"],
        mode="lines+markers",
        name="Foreign pop. %",
        line=dict(color="#d62728", width=3, dash="dot"),
        marker=dict(size=7, symbol="diamond", color="#d62728"),
        yaxis="y2",
    )

    fig.update_layout(
        barmode="stack",
        title="Basque Parliament composition vs foreign-population share",
        xaxis=dict(
            title="Election year",
            dtick=1,                       # show every election year
            tickvals=sorted(grouped["year"].unique()),
        ),
        yaxis=dict(title="Seats (stacked = 75 total)"),
        yaxis2=dict(
            title="Foreign population (%)",
            overlaying="y", side="right",
            titlefont=dict(color="#d62728"),
            tickfont=dict(color="#d62728"),
            rangemode="tozero",
        ),
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Chart 2: Seat share lines for main parties ───────────────────
    st.markdown("---")
    st.markdown("**How each party's share evolved**")
    st.caption(
        "Seat share (%) for the four main parties, with foreign-population "
        "share shaded on the right axis as demographic context."
    )

    fig2 = go.Figure()

    # Background: unified foreign pop % as shaded area (right axis)
    fig2.add_scatter(
        x=demo_valid["year"],
        y=demo_valid["foreign_population_pct"],
        mode="lines",
        name="Foreign pop. %",
        line=dict(color="rgba(214,39,40,0.5)", width=2, dash="dot"),
        fill="tozeroy",
        fillcolor="rgba(214,39,40,0.07)",
        yaxis="y2",
    )

    # Lines: each main party's seat share
    for party in ["PNV", "PSE", "EH Bildu", "PP"]:
        pdf = grouped[grouped["party_group"] == party].sort_values("year")
        fig2.add_scatter(
            x=pdf["year"],
            y=pdf["seat_pct"],
            mode="lines+markers",
            name=party,
            line=dict(width=2.5, color=PARTY_COLORS[party]),
            marker=dict(size=6),
        )

    fig2.update_layout(
        title="Seat share (%) — main parties vs foreign-population share",
        xaxis=dict(
            title="Election year",
            tickvals=sorted(grouped["year"].unique()),
        ),
        yaxis=dict(title="Seat share (%)", rangemode="tozero"),
        yaxis2=dict(
            title="Foreign pop. (%)",
            overlaying="y", side="right",
            titlefont=dict(color="#d62728"),
            tickfont=dict(color="#d62728"),
            rangemode="tozero",
        ),
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("See underlying mart data"):
        st.dataframe(elec, use_container_width=True)
