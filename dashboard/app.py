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

import numpy as np
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
    page_title="Immigration & socioeconomic indicators — Basque Country",
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
    ine = _load_ine_foreign_pop_pct()
    ecp = _load_ecp_foreign_pop_pct()
    if ecp.empty:
        return ine[["year", "foreign_pop_pct_ine"]].rename(
            columns={"foreign_pop_pct_ine": "foreign_population_pct"})
    overlap = ine[["year", "foreign_pop_pct_ine"]].merge(
        ecp[["year", "foreign_pop_pct_ecp"]], on="year")
    if overlap.empty:
        return ine[["year", "foreign_pop_pct_ine"]].rename(
            columns={"foreign_pop_pct_ine": "foreign_population_pct"})
    ratio = (overlap["foreign_pop_pct_ine"] / overlap["foreign_pop_pct_ecp"]).mean()
    result = ine[["year", "foreign_pop_pct_ine"]].rename(
        columns={"foreign_pop_pct_ine": "foreign_population_pct"}).copy()
    padron_max_year = int(ine["year"].max())
    ecp_ext = ecp[ecp["year"] > padron_max_year].copy()
    if not ecp_ext.empty:
        ecp_ext["foreign_population_pct"] = (ecp_ext["foreign_pop_pct_ecp"] * ratio).round(2)
        result = pd.concat([result, ecp_ext[["year", "foreign_population_pct"]]], ignore_index=True)
    return result.sort_values("year")


@st.cache_data(ttl=600, show_spinner="Loading INE Padrón data...")
def _load_ine_foreign_pop_pct() -> pd.DataFrame:
    fp = pd.read_csv(BASE_DIR / "data_clean" / "ine_foreign_population_province.csv")
    fp_agg = fp.groupby("year", as_index=False)["foreign_population"].sum()
    # Total population from BigQuery staging view (covers more years than CSV)
    client = get_bq_client()
    tp_sql = f"""
        SELECT year, SUM(total_population) AS total_population_ine
        FROM `{BQ_PROJECT}.{BQ_DATASET}.stg_ine_population_province`
        GROUP BY year
    """
    tp_agg = client.query(tp_sql).to_dataframe()
    merged = fp_agg.merge(tp_agg, on="year", how="inner")
    merged["foreign_pop_pct_ine"] = (
        merged["foreign_population"] / merged["total_population_ine"] * 100).round(2)
    return merged[["year", "foreign_population", "total_population_ine", "foreign_pop_pct_ine"]]


# ======================================================
# SIDEBAR
# ======================================================

with st.sidebar:
    st.markdown("### Connection")
    st.caption(f"Project: `{BQ_PROJECT}`")
    st.caption(f"Dataset: `{BQ_DATASET}`")
    st.caption("All charts pull from BigQuery on each page load.")
    if st.button("Refresh data", use_container_width=True):
        load_mart.clear()
        st.rerun()
    st.divider()
    st.markdown(
        "Sources: INE, EUSTAT, EPA,\n\n"
        "Interior Ministry, Basque Parliament"
    )


# ======================================================
# HEADER
# ======================================================

st.title("Immigration & socioeconomic indicators — Basque Country")
st.caption(
    "Data sourced from INE, EUSTAT and EPA. "
    "Charts query BigQuery directly on each page load."
)

tab_q1, tab_q2, tab_q3, tab_q4, tab_q5 = st.tabs([
    "Crime & detentions",
    "Labour market",
    "Housing prices",
    "Poverty risk",
    "Elections",
])


# ======================================================
# Q1 — CRIME
# ======================================================

with tab_q1:
    st.subheader("Are foreigners over-represented in police detentions?")
    st.caption(
        "Foreign share of detention events vs foreign share of resident population. "
        "Note: detentions count events, not unique people, and include non-resident foreigners."
    )

    crime = load_mart(MARTS["crime"]).sort_values("year")
    for c in crime.columns:
        if c != "year":
            crime[c] = pd.to_numeric(crime[c], errors="coerce")

    if crime.empty:
        st.error("No crime data returned — check BigQuery mart.")
        st.stop()

    latest = crime.dropna(subset=["over_representation_ratio"]).iloc[-1]
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Year", int(latest["year"]))
    k2.metric("Foreign % of population", f"{latest['foreign_population_pct']:.1f}%")
    k3.metric("Foreign % of detentions", f"{latest['foreign_detention_pct']:.1f}%")
    k4.metric("Ratio", f"{latest['over_representation_ratio']:.1f}x")

    # --- Chart 1: Grouped bar chart — both series ---
    det_series = crime.dropna(subset=["foreign_detention_pct"]).copy()
    pop_series = crime.dropna(subset=["foreign_population_pct"]).copy()

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
        y=pop_series["foreign_population_pct"],
        name="Foreign share of population",
        marker_color="#1f77b4",
        text=[f"{v:.1f}%" for v in pop_series["foreign_population_pct"]],
        textposition="outside", textfont=dict(size=10),
    )
    fig1.update_layout(
        title="Population share vs detention share",
        barmode="group",
        xaxis_title="Year", yaxis_title="Share (%)",
        yaxis=dict(range=[0, 72]),
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig1, use_container_width=True)

    # --- Chart 2: Per-100k detention rates — foreign vs Spanish ---
    st.markdown("---")
    st.markdown("**Detention rate per 100 000 inhabitants**")
    st.caption(
        "How many detention events per 100 000 people in each group. "
        "This is the fairest comparison: it accounts for group size."
    )
    rate_data = crime.dropna(
        subset=["foreign_event_rate_per_100k", "spanish_event_rate_per_100k"]
    ).copy()

    if not rate_data.empty:
        fig_rate = go.Figure()
        fig_rate.add_scatter(
            x=rate_data["year"], y=rate_data["foreign_event_rate_per_100k"],
            mode="lines+markers", name="Foreign",
            line=dict(color="#d62728", width=2.5),
            marker=dict(size=6),
        )
        fig_rate.add_scatter(
            x=rate_data["year"], y=rate_data["spanish_event_rate_per_100k"],
            mode="lines+markers", name="Spanish",
            line=dict(color="#1f77b4", width=2.5),
            marker=dict(size=6),
        )
        fig_rate.update_layout(
            title="Detention events per 100 000 inhabitants",
            xaxis=dict(title="Year"),
            yaxis=dict(title="Events per 100k"),
            hovermode="x unified",
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig_rate, use_container_width=True)

    # --- Chart 3: Crime type breakdown (PV, latest year) ---
    st.markdown("---")
    st.markdown("**What are these detentions actually for?**")
    st.caption(
        "Most detention events in the Basque Country are for property crimes "
        "(theft, robbery), not violent offences. This context matters when "
        "interpreting the over-representation ratio: the gap is driven largely "
        "by low-level property crime, not by serious violent offences."
    )
    crime_type_sql = f"""
        SELECT crime_type, SUM(value) AS detentions
        FROM `{BQ_PROJECT}.{BQ_DATASET}.crime_detentions_total`
        WHERE territory = 'PAÍS VASCO'
          AND sex = 'Ambos sexos'
          AND age_group = 'TOTAL edad'
          AND crime_type != 'TOTAL INFRACCIONES PENALES'
          AND year = (
              SELECT MAX(year)
              FROM `{BQ_PROJECT}.{BQ_DATASET}.crime_detentions_total`
              WHERE territory = 'PAÍS VASCO'
          )
        GROUP BY crime_type
        ORDER BY detentions DESC
    """
    client = get_bq_client()
    crime_types = client.query(crime_type_sql).to_dataframe()

    if not crime_types.empty:
        # Keep only top-level categories (e.g. "5. PATRIMONIO", not "5.1.-Hurtos")
        top_level = crime_types[
            crime_types["crime_type"].str.match(r'^\d+\.\s')
        ].copy()
        # Shorten labels
        top_level["label"] = top_level["crime_type"].str.replace(
            r'^\d+\.\s*', '', regex=True
        ).str.title()
        top_level["detentions"] = pd.to_numeric(top_level["detentions"])
        top_level = top_level.sort_values("detentions", ascending=True)

        fig_ct = go.Figure()
        fig_ct.add_bar(
            y=top_level["label"],
            x=top_level["detentions"],
            orientation="h",
            marker_color="#d62728",
            text=[f"{int(v):,}" for v in top_level["detentions"]],
            textposition="outside",
        )
        fig_ct.update_layout(
            title="Detention events by crime type (latest year)",
            xaxis=dict(title="Detention events"),
            yaxis=dict(title=""),
            margin=dict(l=180),
            showlegend=False,
        )
        st.plotly_chart(fig_ct, use_container_width=True)

    # --- Chart 4: Indexed growth — detentions vs population ---
    st.markdown("---")
    st.markdown("**Are detentions rising because of more immigrants, or higher rates?**")
    st.caption(
        "Foreign detention events (absolute) alongside foreign population. "
        "If both lines grow at the same rate, the per-capita rate stays flat — "
        "the increase is purely demographic, not behavioural."
    )
    idx_data = crime.dropna(
        subset=["foreign_detention_events", "foreign_population"]
    ).copy()

    if not idx_data.empty:
        base_det = float(idx_data["foreign_detention_events"].iloc[0])
        base_pop = float(idx_data["foreign_population"].iloc[0])
        idx_data["det_index"] = (idx_data["foreign_detention_events"] / base_det * 100).round(1)
        idx_data["pop_index"] = (idx_data["foreign_population"] / base_pop * 100).round(1)

        fig_idx = go.Figure()
        fig_idx.add_scatter(
            x=idx_data["year"], y=idx_data["det_index"],
            mode="lines+markers", name="Foreign detentions",
            line=dict(color="#d62728", width=2.5),
            marker=dict(size=6),
        )
        fig_idx.add_scatter(
            x=idx_data["year"], y=idx_data["pop_index"],
            mode="lines+markers", name="Foreign population",
            line=dict(color="#1f77b4", width=2.5),
            marker=dict(size=6),
        )
        fig_idx.add_hline(y=100, line_dash="dash", line_color="gray")
        fig_idx.update_layout(
            title=f"Indexed growth (base {int(idx_data['year'].iloc[0])} = 100)",
            xaxis=dict(title="Year"),
            yaxis=dict(title="Index (100 = base year)"),
            hovermode="x unified",
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig_idx, use_container_width=True)

    with st.expander("Raw data"):
        st.dataframe(crime, use_container_width=True)


# ======================================================
# Q2 — LABOUR
# ======================================================

with tab_q2:
    st.subheader("Are foreign workers more exposed to unemployment?")
    st.caption(
        "Unemployment rates by nationality (EUSTAT) and national-level sector distribution (INE/EPA)."
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

    # --- Charts 1 & 2: Unemployment rate + Activity rate side by side ---
    col_u, col_a = st.columns(2)

    with col_u:
        fig1 = go.Figure()
        for nat, color in [("Foreign", "#d62728"), ("Spanish", "#1f77b4")]:
            sub = labour[labour["nationality"] == nat].sort_values("year")
            fig1.add_scatter(
                x=sub["year"], y=sub["unemployment_rate"],
                mode="lines+markers", name=nat,
                line=dict(color=color, width=2.5),
                marker=dict(size=6),
            )
        # Shade the gap
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
            title="Unemployment rate",
            xaxis_title="Year", yaxis_title="Unemployment rate (%)",
            hovermode="x unified",
            legend=dict(orientation="h", y=-0.25),
            margin=dict(t=50, b=40),
        )
        st.plotly_chart(fig1, use_container_width=True)

    with col_a:
        if "activity_rate" in labour.columns:
            fig_a = go.Figure()
            for nat, color in [("Foreign", "#d62728"), ("Spanish", "#1f77b4")]:
                sub = labour[labour["nationality"] == nat].sort_values("year")
                fig_a.add_scatter(
                    x=sub["year"], y=sub["activity_rate"],
                    mode="lines+markers", name=nat,
                    line=dict(color=color, width=2.5),
                    marker=dict(size=6),
                )
            fig_a.update_layout(
                title="Activity rate (labour participation)",
                xaxis_title="Year", yaxis_title="Activity rate (%)",
                hovermode="x unified",
                legend=dict(orientation="h", y=-0.25),
                margin=dict(t=50, b=40),
            )
            st.plotly_chart(fig_a, use_container_width=True)

    st.caption(
        "Foreign nationals have much higher activity rates (~75%) than Spanish "
        "nationals (~57%). More active job-seeking partly explains the higher "
        "unemployment rate."
    )

    # --- Chart 3: Sector mix — grouped bars ---
    st.markdown("---")
    st.markdown("**Where do foreign workers concentrate?**")
    st.caption(
        "Sector distribution of employment by nationality (national level). "
        "Foreign workers are more concentrated in construction and agriculture "
        "— sectors with higher seasonality and precarity."
    )
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
        title=f"Employment by sector ({latest_year})",
        color_discrete_map={"Foreign": "#d62728", "Spanish": "#1f77b4"},
    )
    fig2.update_layout(yaxis_title="% of employed in sector",
                       legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Raw data"):
        st.dataframe(labour, use_container_width=True)


# ======================================================
# Q3 — HOUSING
# ======================================================

with tab_q3:
    st.subheader("Does immigration growth track housing prices?")
    st.caption(
        "Average price per m² and foreign-population share for each Basque province."
    )

    housing = load_mart(MARTS["housing"]).sort_values(["year", "province_std"])
    for c in housing.columns:
        if c not in ("province_std", "top_foreign_origin"):
            housing[c] = pd.to_numeric(housing[c], errors="coerce")

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
    k1.metric("Latest year", latest_year)
    if not latest.empty:
        k2.metric("Avg PV price", f"{float(latest['avg_price_per_m2'].mean()):.0f} €/m²")
    fp_valid = housing.dropna(subset=["foreign_population_pct"])
    if not fp_valid.empty:
        fp_latest = fp_valid[fp_valid["year"] == fp_valid["year"].max()]
        k3.metric(f"Avg foreign share ({int(fp_latest['year'].iloc[0])})",
                  f"{fp_latest['foreign_population_pct'].mean():.1f}%")
    k4.metric("Avg correlation (r)", f"{avg_corr:.2f}")

    # ── Chart 1: Scatter per province — foreign pop % vs price ──────
    scatter_data = housing.dropna(
        subset=["foreign_population_pct", "avg_price_per_m2"]
    ).copy()

    if not scatter_data.empty:
        sc_cols = st.columns(3)
        for sc_col, prov, color in zip(
            sc_cols,
            ["Araba", "Bizkaia", "Gipuzkoa"],
            ["#1f77b4", "#ff7f0e", "#2ca02c"],
        ):
            sub = scatter_data[scatter_data["province_std"] == prov].sort_values(
                "foreign_population_pct"
            )
            if len(sub) < 3:
                continue

            x = sub["foreign_population_pct"].astype(float).values
            y = sub["avg_price_per_m2"].astype(float).values
            corr_val = sub["avg_price_per_m2"].corr(sub["foreign_population_pct"])
            coeffs = np.polyfit(x, y, 1)
            trend_y = np.polyval(coeffs, x)

            fig_sc = go.Figure()
            fig_sc.add_scatter(
                x=x, y=y,
                mode="markers+text",
                marker=dict(size=10, color=color),
                text=sub["year"].astype(int).astype(str).values,
                textposition="top center",
                textfont=dict(size=9),
                showlegend=False,
            )
            fig_sc.add_scatter(
                x=x, y=trend_y,
                mode="lines",
                line=dict(dash="dash", width=2, color=color),
                showlegend=False,
            )
            fig_sc.update_layout(
                title=f"{prov} (r = {corr_val:.2f})",
                xaxis=dict(title="Foreign pop. (%)"),
                yaxis=dict(title="€ / m²"),
                hovermode="closest",
                margin=dict(t=50, b=40),
            )
            sc_col.plotly_chart(fig_sc, use_container_width=True)

    # ── Chart 2: Year-over-year % change comparison ──────────────────
    st.markdown("---")
    st.markdown("**Do price and immigration move together year to year?**")
    st.caption(
        "Annual % change in housing price vs foreign-population share. "
        "If bars point the same direction in the same year, the two variables "
        "move in sync — not just because both trend upward over time."
    )
    yoy_cols = st.columns(3)
    for yoy_col, prov, color in zip(
        yoy_cols,
        ["Araba", "Bizkaia", "Gipuzkoa"],
        ["#1f77b4", "#ff7f0e", "#2ca02c"],
    ):
        sub = (
            housing[housing["province_std"] == prov]
            .dropna(subset=["avg_price_per_m2", "foreign_population_pct"])
            .sort_values("year")
            .copy()
        )
        if len(sub) < 3:
            continue

        sub["price_chg"] = sub["avg_price_per_m2"].pct_change() * 100
        sub["fp_chg"] = sub["foreign_population_pct"].pct_change() * 100
        sub = sub.dropna(subset=["price_chg", "fp_chg"])

        fig_yoy = go.Figure()
        fig_yoy.add_bar(
            x=sub["year"], y=sub["price_chg"],
            name="Price change",
            marker_color=color,
            opacity=0.7,
        )
        fig_yoy.add_bar(
            x=sub["year"], y=sub["fp_chg"],
            name="Foreign pop. change",
            marker_color="#999999",
            opacity=0.7,
        )
        fig_yoy.add_hline(y=0, line_dash="dash", line_color="gray")
        fig_yoy.update_layout(
            title=prov,
            barmode="group",
            xaxis=dict(title="Year"),
            yaxis=dict(title="Change (%)"),
            legend=dict(orientation="h", y=-0.25),
            margin=dict(t=50, b=40),
        )
        yoy_col.plotly_chart(fig_yoy, use_container_width=True)

    # ── Chart 3: Indexed evolution — price vs foreign pop (base = first year) ──
    st.markdown("---")
    st.markdown("**Do prices and immigration track each other over time?**")
    st.caption(
        "Both series indexed to 100 at the first overlapping year. "
        "If the lines move together, the two variables evolve in sync. "
        "Plotted on the same scale — no dual-axis tricks."
    )
    idx_cols = st.columns(3)
    for idx_col, prov, color in zip(
        idx_cols,
        ["Araba", "Bizkaia", "Gipuzkoa"],
        ["#1f77b4", "#ff7f0e", "#2ca02c"],
    ):
        sub = (
            housing[housing["province_std"] == prov]
            .dropna(subset=["avg_price_per_m2", "foreign_population_pct"])
            .sort_values("year")
            .copy()
        )
        if len(sub) < 3:
            continue

        base_price = float(sub["avg_price_per_m2"].iloc[0])
        base_fp = float(sub["foreign_population_pct"].iloc[0])
        sub["price_idx"] = (sub["avg_price_per_m2"] / base_price * 100).round(1)
        sub["fp_idx"] = (sub["foreign_population_pct"] / base_fp * 100).round(1)

        fig_idx = go.Figure()
        fig_idx.add_scatter(
            x=sub["year"], y=sub["price_idx"],
            mode="lines+markers", name="Price €/m²",
            line=dict(color=color, width=2.5),
            marker=dict(size=6),
        )
        fig_idx.add_scatter(
            x=sub["year"], y=sub["fp_idx"],
            mode="lines+markers", name="Foreign pop. %",
            line=dict(color="#999999", width=2.5, dash="dot"),
            marker=dict(size=6),
        )
        fig_idx.add_hline(y=100, line_dash="dash", line_color="lightgray")
        fig_idx.update_layout(
            title=prov,
            xaxis=dict(title="Year"),
            yaxis=dict(title="Index (100 = base year)"),
            hovermode="x unified",
            legend=dict(orientation="h", y=-0.25),
            margin=dict(t=50, b=40),
        )
        idx_col.plotly_chart(fig_idx, use_container_width=True)

    with st.expander("Raw data"):
        st.dataframe(housing, use_container_width=True)


# ======================================================
# Q4 — POVERTY
# ======================================================

with tab_q4:
    st.subheader("Does immigration growth track poverty levels?")
    st.caption(
        "Basque poverty-risk rate (INE) alongside the foreign-population share, "
        "with the national rate shown for comparison."
    )

    # ── Build poverty dataset from CSV (bypasses mart for national rate) ──
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

    poverty = pv_pov.merge(nat_pov, on="year", how="left")

    demo_full = _load_demographics_full()
    poverty = poverty.merge(
        demo_full[["year", "foreign_population_pct"]],
        on="year", how="left"
    )
    poverty = poverty.sort_values("year")
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
    k1.metric("Latest year", latest_year)
    k2.metric("Basque poverty rate", f"{float(latest['poverty_rate']):.1f}%",
              delta=f"{float(latest['poverty_rate']) - float(poverty.iloc[0]['poverty_rate']):+.1f} pp since {int(poverty.iloc[0]['year'])}",
              delta_color="inverse")
    if pd.notna(latest.get("national_poverty_rate")):
        k3.metric("Spain national rate", f"{float(latest['national_poverty_rate']):.1f}%",
                  delta=f"{float(latest.get('gap_pp', 0)):.1f} pp lower in Basque C.",
                  delta_color="normal")
    if fp_latest is not None:
        k4.metric(f"Foreign share ({int(fp_latest['year'])})",
                  f"{float(fp_latest['foreign_population_pct']):.1f}%",
                  help=f"Chain-linked INE series, r = {corr:.2f}")

    # ── Chart 1: Scatter — foreign pop % vs poverty rate ────────────
    sc_data = poverty.dropna(subset=["poverty_rate", "foreign_population_pct"]).copy()

    sc1, sc2 = st.columns(2)

    with sc1:
        if len(sc_data) >= 3:
            x = sc_data["foreign_population_pct"].astype(float).values
            y = sc_data["poverty_rate"].astype(float).values
            coeffs = np.polyfit(x, y, 1)

            fig_sc = go.Figure()
            fig_sc.add_scatter(
                x=x, y=y,
                mode="markers+text",
                marker=dict(size=10, color="#1f77b4"),
                text=sc_data["year"].astype(int).astype(str).values,
                textposition="top center",
                textfont=dict(size=9),
                showlegend=False,
            )
            fig_sc.add_scatter(
                x=np.sort(x),
                y=np.polyval(coeffs, np.sort(x)),
                mode="lines",
                line=dict(dash="dash", width=2, color="#1f77b4"),
                showlegend=False,
            )
            fig_sc.update_layout(
                title=f"Foreign pop. % vs poverty rate (r = {corr:.2f})",
                xaxis=dict(title="Foreign population (%)"),
                yaxis=dict(title="Poverty rate (%)"),
                hovermode="closest",
                margin=dict(t=50, b=40),
            )
            st.plotly_chart(fig_sc, use_container_width=True)

    # ── Chart 2: YoY % change comparison ─────────────────────────────
    with sc2:
        yoy = sc_data.sort_values("year").copy()
        yoy["poverty_chg"] = yoy["poverty_rate"].pct_change() * 100
        yoy["fp_chg"] = yoy["foreign_population_pct"].pct_change() * 100
        yoy = yoy.dropna(subset=["poverty_chg", "fp_chg"])

        if not yoy.empty:
            fig_yoy = go.Figure()
            fig_yoy.add_bar(
                x=yoy["year"], y=yoy["poverty_chg"],
                name="Poverty rate change",
                marker_color="#1f77b4",
                opacity=0.7,
            )
            fig_yoy.add_bar(
                x=yoy["year"], y=yoy["fp_chg"],
                name="Foreign pop. change",
                marker_color="#999999",
                opacity=0.7,
            )
            fig_yoy.add_hline(y=0, line_dash="dash", line_color="gray")
            fig_yoy.update_layout(
                title="Annual % change: poverty vs foreign share",
                barmode="group",
                xaxis=dict(title="Year"),
                yaxis=dict(title="Change (%)"),
                legend=dict(orientation="h", y=-0.25),
                margin=dict(t=50, b=40),
            )
            st.plotly_chart(fig_yoy, use_container_width=True)

    # ── Chart 3: Indexed evolution — poverty vs foreign pop ──────────
    st.markdown("---")
    st.markdown("**Do poverty and immigration track each other over time?**")
    st.caption(
        "Both series indexed to 100 at the first overlapping year. "
        "If lines move together, they evolve in sync. Same scale — no dual-axis."
    )
    idx_data = sc_data.sort_values("year").copy()
    if len(idx_data) >= 3:
        base_pov = float(idx_data["poverty_rate"].iloc[0])
        base_fp = float(idx_data["foreign_population_pct"].iloc[0])
        idx_data["pov_idx"] = (idx_data["poverty_rate"] / base_pov * 100).round(1)
        idx_data["fp_idx"] = (idx_data["foreign_population_pct"] / base_fp * 100).round(1)

        fig_idx = go.Figure()
        fig_idx.add_scatter(
            x=idx_data["year"], y=idx_data["pov_idx"],
            mode="lines+markers", name="Basque poverty rate",
            line=dict(color="#1f77b4", width=2.5),
            marker=dict(size=6),
        )
        fig_idx.add_scatter(
            x=idx_data["year"], y=idx_data["fp_idx"],
            mode="lines+markers", name="Foreign pop. %",
            line=dict(color="#999999", width=2.5, dash="dot"),
            marker=dict(size=6),
        )
        fig_idx.add_hline(y=100, line_dash="dash", line_color="lightgray")
        fig_idx.update_layout(
            title=f"Indexed evolution (base {int(idx_data['year'].iloc[0])} = 100)",
            xaxis=dict(title="Year"),
            yaxis=dict(title="Index (100 = base year)"),
            hovermode="x unified",
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig_idx, use_container_width=True)

    # ── Chart 4: Basque vs national poverty rate over time ───────────
    st.markdown("---")
    st.markdown("**Is Basque poverty driven by local immigration or national trends?**")
    st.caption(
        "If the Basque rate follows the national trend closely, then poverty "
        "changes are likely driven by macro-economic factors common to all of "
        "Spain — not by local immigration patterns."
    )
    pov_both = poverty.dropna(subset=["poverty_rate", "national_poverty_rate"]).copy()
    if not pov_both.empty:
        fig_comp = go.Figure()
        fig_comp.add_scatter(
            x=pov_both["year"], y=pov_both["poverty_rate"],
            mode="lines+markers", name="Basque Country",
            line=dict(color="#1f77b4", width=2.5),
            marker=dict(size=6),
        )
        fig_comp.add_scatter(
            x=pov_both["year"], y=pov_both["national_poverty_rate"],
            mode="lines+markers", name="Spain national",
            line=dict(color="#aec7e8", width=2.5, dash="dash"),
            marker=dict(size=6),
        )
        fig_comp.update_layout(
            title="Poverty rate: Basque Country vs Spain",
            xaxis=dict(title="Year"),
            yaxis=dict(title="Poverty rate (%)"),
            hovermode="x unified",
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig_comp, use_container_width=True)
        st.caption(
            "Both rates follow similar peaks and troughs (2008 crisis, COVID), "
            "suggesting poverty is shaped more by national economic cycles than "
            "by local immigration levels."
        )

    with st.expander("Raw data"):
        st.dataframe(poverty, use_container_width=True)


# ======================================================
# Q5 — ELECTIONS
# ======================================================

with tab_q5:
    st.subheader("Do election results shift as demographics change?")
    st.caption(
        "Basque Parliament seat distribution over time, with the foreign-population "
        "share overlaid for context."
    )
    elec = load_mart(MARTS["elections"])

    # Ensure year is integer and sort chronologically
    elec["year"] = elec["year"].astype(int)
    elec = elec.sort_values(["year", "seats"], ascending=[True, False])

    # Keep only election years with demographic data (1998 onwards)
    elec = elec[elec["year"] >= 1998]

    # ── Consolidate parties ──────────────────────────────────────────
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

    # Load continuous demographic series from dbt intermediate model
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
    k1.metric("Latest election", int(elec["year"].max()))
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
    PARTY_COLORS = {
        "PNV":      "#2ca02c",
        "PSE":      "#d62728",
        "EH Bildu": "#ff7f0e",
        "PP":       "#1f77b4",
        "Others":   "#cccccc",
    }
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

    # Overlay: continuous foreign population % (right axis)
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
        title="Basque Parliament seats and foreign-population share",
        xaxis=dict(
            title="Election year",
            dtick=1,
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

    # ── Chart 2: Scatter — foreign pop % vs seat share per party ────
    st.markdown("---")
    st.markdown("**Does any party's seat share correlate with immigration?**")
    st.caption(
        "Each dot is one election year. A positive slope means the party "
        "tends to win more seats as immigration grows; negative means the opposite."
    )

    # Merge grouped seats with foreign pop % at each election year
    fp_map = demo_full.set_index("year")["foreign_population_pct"].to_dict()
    grouped["foreign_pct"] = grouped["year"].map(fp_map)
    scatter_elec = grouped.dropna(subset=["foreign_pct"]).copy()

    if not scatter_elec.empty:
        sc_cols = st.columns(4)
        for sc_col, party, color in zip(
            sc_cols,
            ["PNV", "PSE", "EH Bildu", "PP"],
            ["#2ca02c", "#d62728", "#ff7f0e", "#1f77b4"],
        ):
            sub = scatter_elec[scatter_elec["party_group"] == party].sort_values(
                "foreign_pct"
            )
            if len(sub) < 3:
                continue

            x = sub["foreign_pct"].astype(float).values
            y = sub["seat_pct"].astype(float).values
            corr_val = sub["seat_pct"].corr(sub["foreign_pct"])
            coeffs = np.polyfit(x, y, 1)
            trend_y = np.polyval(coeffs, np.sort(x))

            fig_sc = go.Figure()
            fig_sc.add_scatter(
                x=x, y=y,
                mode="markers+text",
                marker=dict(size=10, color=color),
                text=sub["year"].astype(int).astype(str).values,
                textposition="top center",
                textfont=dict(size=9),
                showlegend=False,
            )
            fig_sc.add_scatter(
                x=np.sort(x), y=trend_y,
                mode="lines",
                line=dict(dash="dash", width=2, color=color),
                showlegend=False,
            )
            fig_sc.update_layout(
                title=f"{party} (r = {corr_val:.2f})",
                xaxis=dict(title="Foreign pop. (%)"),
                yaxis=dict(title="Seat share (%)"),
                hovermode="closest",
                margin=dict(t=50, b=40),
            )
            sc_col.plotly_chart(fig_sc, use_container_width=True)

    with st.expander("Raw data"):
        elec_display = elec.copy()
        fp_map = demo_full.set_index("year")["foreign_population_pct"].to_dict()
        elec_display["foreign_population_pct"] = (
            elec_display["year"].map(fp_map)
        )
        show_cols = [
            "year", "party_name", "seats", "total_seats_year",
            "seats_share_pct", "foreign_population_pct",
        ]
        show_cols = [c for c in show_cols if c in elec_display.columns]
        st.dataframe(elec_display[show_cols], use_container_width=True)
