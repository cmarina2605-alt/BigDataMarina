"""
Dashboard builder — replicates the 5 dbt marts in pandas (reading from
data_clean/) and emits one self-contained `dashboard.html` with interactive
Plotly charts.  Open the HTML file in any browser; no server, no installs.

Run:
    python build_dashboard.py

Output:
    dashboard.html   (single self-contained file, ~Plotly via CDN)

The mart logic here mirrors the dbt models in models/ one-to-one, so this
script is effectively a local preview of what BigQuery would return.
"""

from __future__ import annotations
import json
import os
from pathlib import Path

import pandas as pd

# ----------------------------------------------------------------------
# Paths
# ----------------------------------------------------------------------
ROOT  = Path(__file__).parent
CLEAN = ROOT / "data_clean"
OUT   = ROOT / "dashboard.html"


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def read(name: str) -> pd.DataFrame:
    return pd.read_csv(CLEAN / name, encoding="utf-8-sig")


def std_province(p: str) -> str:
    if pd.isna(p):
        return p
    p = p.strip()
    if p.lower().startswith("araba") or p.lower().startswith("álava") or p.lower().startswith("alava"):
        return "Araba"
    if p.lower().startswith("bizkaia"):
        return "Bizkaia"
    if p.lower().startswith("gipuzkoa"):
        return "Gipuzkoa"
    if p in ("Basque Country", "A. C. of Euskadi", "País Vasco", "Total"):
        return "País Vasco"
    return p


# ----------------------------------------------------------------------
# Base building blocks (mirrors stg + int layer)
# ----------------------------------------------------------------------
def population_by_province() -> pd.DataFrame:
    """Mirrors stg_ine_population_province: total residents per (province, year)."""
    df = read("population_territory.csv")
    df = df[
        (df["level"] == "province")
        & (df["territory"].isin(["Araba/Álava", "Bizkaia", "Gipuzkoa"]))
        & (df["sex"] == "Ambos sexos")
        & (df["age_group"] == "Todas las edades")
        & (df["periodo"] == "1 de julio de")
    ].copy()
    df["province_std"] = df["territory"].map(std_province)
    df["year"] = df["year"].astype(int)
    df["total_population"] = df["population"].astype(float).round().astype(int)
    return df[["year", "province_std", "total_population"]]


def foreign_by_province() -> pd.DataFrame:
    """Mirrors stg_eustat_foreign_population_province: foreign residents (all
    continents summed) per (province, year).  The source only has foreign
    people (continent labels), so summing non-'Total' rows == total foreign.
    """
    df = read("eustat_population_nationality_clean.csv")
    df = df[
        (df["province"].isin(["Araba/Alava", "Bizkaia", "Gipuzkoa"]))
        & (df["relation_with_activity"] == "Total")
        & (df["sex"] == "Total")
        & (df["nationality"] == "Total")
    ].copy()
    df["province_std"] = df["province"].map(
        lambda x: "Araba" if x == "Araba/Alava" else x
    )
    df["year"] = df["year"].astype(int)
    df = df.rename(columns={"population": "foreign_population"})
    df["foreign_population"] = df["foreign_population"].astype(int)
    return df[["year", "province_std", "foreign_population"]]


def foreign_pct_by_province() -> pd.DataFrame:
    """Mirrors int_foreign_population_pct."""
    total = population_by_province()
    foreign = foreign_by_province()
    df = total.merge(foreign, on=["year", "province_std"], how="inner")
    df["foreign_population_pct"] = (
        df["foreign_population"] / df["total_population"] * 100
    ).round(2)
    return df


def foreign_pct_basque() -> pd.DataFrame:
    """Mirrors int_population_pais_vasco_year."""
    df = (
        foreign_pct_by_province()
        .groupby("year")
        .agg(
            total_population=("total_population", "sum"),
            foreign_population=("foreign_population", "sum"),
        )
        .reset_index()
    )
    df["foreign_population_pct"] = (
        df["foreign_population"] / df["total_population"] * 100
    ).round(2)
    return df


# ----------------------------------------------------------------------
# Mart 1 — Crime rates by nationality (RQ1)
# ----------------------------------------------------------------------
def mart_crime() -> pd.DataFrame:
    # Total detentions in PV — 'TOTAL edad' is the already-aggregated age row,
    # sex='Ambos sexos' is the headline slice.
    tot = read("crime_total_clean.csv")
    tot = tot[
        (tot["territory"] == "PAÍS VASCO")
        & (tot["crime_type"] == "TOTAL INFRACCIONES PENALES")
        & (tot["age_group"] == "TOTAL edad")
        & (tot["sex"] == "Ambos sexos")
    ].copy()
    tot["year"] = tot["year"].astype(int)
    tot = tot.groupby("year")["value"].sum().reset_index(name="total_detentions_pv")

    # Foreign detentions in PV — crime_detentions has one row per country, no
    # aggregates, so summing over all country rows for the three provinces and
    # 'Ambos sexos' gives the total foreign-national detentions.
    foreign = read("crime_detentions_clean.csv")
    foreign = foreign[
        foreign["province"].isin(["Araba/Álava", "Bizkaia", "Gipuzkoa"])
        & (foreign["sex"] == "Ambos sexos")
    ].copy()
    foreign["year"] = foreign["year"].astype(int)
    foreign = (
        foreign.groupby("year")["value"].sum().reset_index(name="foreign_detentions_pv")
    )

    # PV population by year
    pop = foreign_pct_basque()[
        ["year", "total_population", "foreign_population"]
    ]
    pop["spanish_population"] = pop["total_population"] - pop["foreign_population"]

    # Join
    df = tot.merge(pop, on="year", how="inner").merge(foreign, on="year", how="inner")
    df["spanish_detentions_pv"] = df["total_detentions_pv"] - df["foreign_detentions_pv"]

    # Shares — the defensible, scale-invariant framing
    df["foreign_pct_of_population"] = (
        df["foreign_population"] / df["total_population"] * 100
    ).round(2)
    df["foreign_pct_of_detentions"] = (
        df["foreign_detentions_pv"] / df["total_detentions_pv"] * 100
    ).round(2)
    df["over_representation_ratio"] = (
        df["foreign_pct_of_detentions"] / df["foreign_pct_of_population"]
    ).round(2)

    # Event-rates kept for completeness (labelled, not headline)
    df["event_rate_total"] = (
        df["total_detentions_pv"] / df["total_population"] * 100_000
    ).round(2)
    df["event_rate_foreign"] = (
        df["foreign_detentions_pv"] / df["foreign_population"] * 100_000
    ).round(2)
    df["event_rate_spanish"] = (
        df["spanish_detentions_pv"] / df["spanish_population"] * 100_000
    ).round(2)
    return df.sort_values("year")


# ----------------------------------------------------------------------
# Mart 2 — Labour market by nationality (RQ2)
# ----------------------------------------------------------------------
def mart_labour() -> pd.DataFrame:
    act = read("eustat_activity_nationality_clean.csv")
    # province='A. C. of Euskadi' is PV-level; nationality in {Spanish, Foreign}
    act = act[
        (act["province"] == "A. C. of Euskadi")
        & (act["quarter"] == "Annual average")
        & (act["nationality"].isin(["Spanish nationality", "Foreign nationality"]))
    ].copy()
    act["nationality"] = act["nationality"].map(
        {"Spanish nationality": "Spanish", "Foreign nationality": "Foreign"}
    )
    act["year"] = act["year"].astype(int)
    rates = act.pivot_table(
        index=["year", "nationality"],
        columns="rate_type",
        values="value",
        aggfunc="mean",
    ).reset_index()
    rates.columns.name = None
    rates = rates.rename(
        columns={
            "Activity rate":     "activity_rate",
            "Employment rate":   "employment_rate",
            "Unemployment rate": "unemployment_rate",
        }
    )

    # Volumes (Basque Country level, summing provinces)
    vol = read("eustat_population_nationality_clean.csv")
    vol = vol[
        (vol["province"].isin(["Araba/Alava", "Bizkaia", "Gipuzkoa"]))
        & (vol["sex"] == "Total")
        & (vol["nationality"] == "Total")
    ].copy()
    vol["year"] = vol["year"].astype(int)
    vol = (
        vol.pivot_table(
            index="year",
            columns="relation_with_activity",
            values="population",
            aggfunc="sum",
        )
        .reset_index()
        .rename(
            columns={
                "Employed population":   "foreign_employed",
                "Unemployed population": "foreign_unemployed",
                "Inactive population":   "foreign_inactive",
                "Total":                 "foreign_total",
            }
        )
    )
    vol.columns.name = None

    # For the dashboard we keep rates only; volumes used for text callouts.
    return rates.sort_values(["year", "nationality"]), vol.sort_values("year")


def mart_labour_sectors() -> pd.DataFrame:
    """National sectoral mix, collapsed to Spanish / Foreign."""
    df = read("ine_employment_clean.csv")
    df = df[
        (df["relation_activity"] == "Ocupados")
        & (df["territory"] == "Total Nacional")
        & (df["sex"] == "Ambos sexos")
        & (df["sector"].isin(["Agricultura", "Industria", "Construcción", "Servicios", "Total"]))
        & (df["nationality"] != "Extranjera: Total")
    ].copy()
    foreign_labels = {
        "Extranjera: Unión Europea",
        "Extranjera: América Latina",
        "Extranjera: Resto de Europa",
        "Extranjera: Resto del mundo y apátrida",
    }
    def bucket(n):
        if n == "Total":
            return "Total"
        if n in ("Española", "Doble nacionalidad"):
            return "Spanish"
        if n in foreign_labels:
            return "Foreign"
        return "Other"
    df["nationality_bucket"] = df["nationality"].map(bucket)
    df = df[df["nationality_bucket"].isin(["Spanish", "Foreign"])]
    df["year"] = df["year"].astype(int)
    agg = (
        df.groupby(["year", "sector", "nationality_bucket"])["value"]
        .sum()
        .reset_index()
    )
    # pivot so sector_pct can be computed per (year, nationality)
    wide = agg.pivot_table(
        index=["year", "nationality_bucket"], columns="sector", values="value"
    ).reset_index()
    wide.columns.name = None
    for s in ("Agricultura", "Industria", "Construcción", "Servicios"):
        wide[f"pct_{s.lower()}"] = (wide[s] / wide["Total"] * 100).round(2)
    return wide.sort_values(["year", "nationality_bucket"])


# ----------------------------------------------------------------------
# Mart 3 — Housing vs foreign share (RQ3)
# ----------------------------------------------------------------------
def mart_housing() -> pd.DataFrame:
    h = read("housing_prices_annual.csv")
    h["province_std"] = h["province"].map(std_province)
    h["year"] = h["year"].astype(int)
    h = (
        h[h["province_std"].isin(["Araba", "Bizkaia", "Gipuzkoa"])]
        .groupby(["year", "province_std"])["price_per_m2"]
        .mean()
        .reset_index()
    )
    fp = foreign_pct_by_province()
    return h.merge(fp, on=["year", "province_std"], how="left").sort_values(
        ["year", "province_std"]
    )


def housing_origin_mix() -> pd.DataFrame:
    """Origin mix per province from eustat_population_clean, latest available year."""
    df = read("eustat_population_clean.csv")
    _origin_en = {
        "América del Sur": "South America",
        "Asia y Oceanía": "Asia & Oceania",
        "Europa del Este": "Eastern Europe",
        "Paises del Magreb": "Maghreb",
        "Resto de Africa": "Rest of Africa",
        "Resto de América": "Rest of Americas",
        "Resto de Europa": "Rest of Europe",
    }
    df = df[df["nationality"].isin(_origin_en.keys())].copy()
    df["nationality"] = df["nationality"].map(_origin_en)
    df["province_std"] = df["province"].map(std_province)
    df["year"] = df["year"].astype(int)
    df["population"] = df["population"].astype(int)
    df = df[df["province_std"].isin(["Araba", "Bizkaia", "Gipuzkoa"])]
    # Keep only the latest year to avoid mixing data across years
    df = df[df["year"] == df["year"].max()]
    # compute share within each (province, year)
    df["foreign_total"] = df.groupby(["province_std", "year"])["population"].transform("sum")
    df["pct_of_foreign"] = (df["population"] / df["foreign_total"] * 100).round(2)
    return df.sort_values(["province_std", "year", "population"], ascending=[True, True, False])


# ----------------------------------------------------------------------
# Mart 4 — Poverty vs foreign share (RQ4)
# ----------------------------------------------------------------------
def mart_poverty() -> pd.DataFrame:
    df = read("ine_pobreza.csv")
    df = df[
        (df["territory"] == "País Vasco")
        & df["indicator"].str.contains("Tasa de riesgo de pobreza")
        & ~df["indicator"].str.contains("alquiler imputado")
        & df["indicator"].str.contains("Base 2013")
    ].copy()
    df["year"] = df["year"].astype(int)
    df = df[["year", "poverty_rate"]].rename(columns={"poverty_rate": "poverty_rate_pv"})
    df["poverty_rate_pv"] = df["poverty_rate_pv"].astype(float)
    pop = foreign_pct_basque()
    return df.merge(pop, on="year", how="inner").sort_values("year")


# ----------------------------------------------------------------------
# Mart 5 — Elections vs demographics (RQ5)
# ----------------------------------------------------------------------
def mart_elections() -> pd.DataFrame:
    el = read("elections_clean.csv")
    el["year"] = el["year"].astype(int)
    el["seats"] = el["seats"].astype(int)

    # The source has multiple monthly composition snapshots per year.
    # Keep only the latest month per year for a consistent snapshot.
    _month_order = {
        "enero": 1, "ene.": 1, "feb.": 2, "febr.": 2, "marzo": 3,
        "abril": 4, "mayo": 5, "junio": 6, "julio": 7,
        "sept.": 9, "oct.": 10, "nov.": 11, "dic.": 12,
    }
    el["_mnum"] = el["month"].str.strip().str.lower().map(_month_order).fillna(6).astype(int)
    latest_month = el.groupby("year")["_mnum"].max().reset_index(name="_max_m")
    el = el.merge(latest_month, on="year")
    el = el[el["_mnum"] == el["_max_m"]].drop(columns=["_mnum", "_max_m"])

    el["party"] = el["party_name"].astype(str).str.strip().str.upper()
    agg = el.groupby(["year", "party"])["seats"].sum().reset_index()
    tot = agg.groupby("year")["seats"].sum().reset_index(name="total_seats_year")
    out = agg.merge(tot, on="year")
    out["seats_share_pct"] = (out["seats"] / out["total_seats_year"] * 100).round(2)

    pop = foreign_pct_basque()
    out = out.merge(pop[["year", "foreign_population_pct"]], on="year", how="left")

    mig = read("birth_country_clean.csv")
    mig = mig[
        (mig["region"] == "País Vasco")
        & (mig["indicator"] == "Tasa de Inmigración Interautonómica")
        & (mig["sex"] == "Ambos sexos")
        & (mig["age_group"] == "Total edades")
        & (mig["periodicity"] == "Anual")
        & (mig["data_type"] == "Dato base")
    ].copy()
    mig["year"] = mig["year"].astype(int)
    mig_w = mig.pivot_table(
        index="year", columns="nationality", values="value", aggfunc="mean"
    ).reset_index()
    mig_w.columns.name = None
    mig_w = mig_w.rename(
        columns={
            "Ambas nacionalidades": "in_migration_rate_total",
            "Española": "in_migration_rate_spanish",
            "Extranjera": "in_migration_rate_foreign",
        }
    )
    return out.merge(mig_w, on="year", how="left"), mig_w


# ----------------------------------------------------------------------
# HTML rendering helpers
# ----------------------------------------------------------------------
def chart(fig_id: str, spec: dict) -> str:
    """Render a single Plotly div plus its JSON spec."""
    data_json = json.dumps(spec["data"])
    layout_json = json.dumps(spec.get("layout", {}))
    return f"""
<div id="{fig_id}" class="chart"></div>
<script>
  Plotly.newPlot({json.dumps(fig_id)}, {data_json}, {layout_json}, {{responsive:true, displaylogo:false}});
</script>
"""


def trace_line(df, x, y, name, mode="lines+markers", yaxis="y"):
    return {
        "type": "scatter",
        "mode": mode,
        "x": df[x].tolist(),
        "y": df[y].tolist(),
        "name": name,
        "yaxis": yaxis,
    }


def trace_bar(df, x, y, name, yaxis="y"):
    return {
        "type": "bar",
        "x": df[x].tolist(),
        "y": df[y].tolist(),
        "name": name,
        "yaxis": yaxis,
    }


# ----------------------------------------------------------------------
# Build the page
# ----------------------------------------------------------------------
def build_html() -> str:
    # --- compute marts
    crime    = mart_crime()
    labour, labour_vol = mart_labour()
    sectors  = mart_labour_sectors()
    housing  = mart_housing()
    origin   = housing_origin_mix()
    poverty  = mart_poverty()
    elec, migration = mart_elections()

    latest_year_labour = int(labour["year"].max())

    # --- Q1 charts
    # Two clean lines: foreign share of detentions vs foreign share of population.
    # The persistent gap between them IS the over-representation.  The exact
    # ratio is already shown in the KPI cards above the chart.
    spec_q1 = {
        "data": [
            trace_line(crime, "year", "foreign_pct_of_detentions", "% of detentions that are foreign"),
            trace_line(crime, "year", "foreign_pct_of_population", "% of population that is foreign"),
        ],
        "layout": {
            "title": "Foreign share: detentions vs population — Basque Country",
            "xaxis": {"title": "Year"},
            "yaxis":  {"title": "Share (%)"},
            "hovermode": "x unified",
            "legend": {"orientation": "h", "y": -0.2},
        },
    }

    # --- Q2 charts
    labour_wide = labour.pivot(
        index="year", columns="nationality", values="unemployment_rate"
    ).reset_index()
    spec_q2a = {
        "data": [
            trace_line(labour_wide, "year", "Spanish", "Spanish nationals"),
            trace_line(labour_wide, "year", "Foreign", "Foreign residents"),
        ],
        "layout": {
            "title": "Unemployment rate by nationality — Basque Country",
            "xaxis": {"title": "Year"},
            "yaxis": {"title": "Unemployment rate (%)"},
            "hovermode": "x unified",
            "legend": {"orientation": "h", "y": -0.2},
        },
    }
    # sector mix in latest available year (national backdrop)
    latest_sector = sectors[sectors["year"] == sectors["year"].max()]
    sector_cols = ["pct_agricultura", "pct_industria", "pct_construcción", "pct_servicios"]
    sector_labels = ["Agriculture", "Industry", "Construction", "Services"]
    spec_q2b = {
        "data": [
            {
                "type": "bar",
                "x": sector_labels,
                "y": latest_sector[latest_sector["nationality_bucket"] == "Spanish"][sector_cols]
                    .iloc[0].tolist(),
                "name": "Spanish nationals",
            },
            {
                "type": "bar",
                "x": sector_labels,
                "y": latest_sector[latest_sector["nationality_bucket"] == "Foreign"][sector_cols]
                    .iloc[0].tolist(),
                "name": "Foreign residents",
            },
        ],
        "layout": {
            "title": f"National sector mix of employment — {int(latest_sector['year'].max())}",
            "barmode": "group",
            "yaxis": {"title": "% of employed in sector"},
            "legend": {"orientation": "h", "y": -0.2},
        },
    }

    # --- Q3 charts
    # One subplot per province so the price / foreign-% correlation is easy
    # to read without 6 tangled lines.  Each subplot has dual y-axes.
    # Interpolate the foreign-pop gap (2017) and restrict to years where
    # both metrics overlap so lines don't break off.
    q3a_traces = []
    q3a_annotations = []
    provinces = ["Araba", "Bizkaia", "Gipuzkoa"]
    for i, prov in enumerate(provinces, start=1):
        sub = housing[housing["province_std"] == prov].sort_values("year").copy()
        # Interpolate small gaps in foreign_population_pct (e.g. 2017)
        sub["foreign_population_pct"] = sub["foreign_population_pct"].interpolate(method="linear")
        # Keep only years where both metrics exist (no extrapolation)
        sub = sub.dropna(subset=["price_per_m2", "foreign_population_pct"])

        xa = f"x{i}" if i > 1 else "x"
        ya_price = f"y{2*i - 1}" if i > 1 else "y"
        ya_pct   = f"y{2*i}"

        q3a_traces.append({
            "type": "scatter", "mode": "lines+markers",
            "x": sub["year"].tolist(), "y": sub["price_per_m2"].tolist(),
            "name": "Price €/m²", "legendgroup": "price",
            "showlegend": i == 1,
            "marker": {"color": "#1f77b4"},
            "xaxis": xa, "yaxis": ya_price,
        })
        q3a_traces.append({
            "type": "scatter", "mode": "lines+markers",
            "x": sub["year"].tolist(), "y": [round(v, 2) for v in sub["foreign_population_pct"].tolist()],
            "name": "Foreign pop. %", "legendgroup": "fpct",
            "showlegend": i == 1,
            "marker": {"color": "#ff7f0e"},
            "xaxis": xa, "yaxis": ya_pct,
        })
        q3a_annotations.append({
            "text": f"<b>{prov}</b>", "showarrow": False,
            "xref": "paper", "yref": "paper",
            "x": (i - 1) / 3 + 1 / 6, "y": 1.06,
            "font": {"size": 14},
        })

    # Shared x-range across subplots (based on overlapping years only)
    overlap_years = housing.dropna(subset=["price_per_m2", "foreign_population_pct"])["year"]
    x_range = [int(overlap_years.min()) - 0.5, int(overlap_years.max()) + 0.5]

    q3a_layout = {
        "title": "Housing price (€/m²) vs foreign-population share — by province",
        "hovermode": "x unified",
        "legend": {"orientation": "h", "y": -0.15},
        "annotations": q3a_annotations,
        # Araba (left third)
        "xaxis":  {"domain": [0, 0.30], "range": x_range, "title": "Year"},
        "yaxis":  {"title": "€/m²", "titlefont": {"color": "#1f77b4"}, "tickfont": {"color": "#1f77b4"}},
        "yaxis2": {"overlaying": "y", "side": "right",
                   "title": "Foreign %", "titlefont": {"color": "#ff7f0e"}, "tickfont": {"color": "#ff7f0e"}},
        # Bizkaia (centre third)
        "xaxis2": {"domain": [0.36, 0.66], "range": x_range, "title": "Year"},
        "yaxis3": {"anchor": "x2", "title": "€/m²", "titlefont": {"color": "#1f77b4"}, "tickfont": {"color": "#1f77b4"}},
        "yaxis4": {"anchor": "x2", "overlaying": "y3", "side": "right",
                   "title": "Foreign %", "titlefont": {"color": "#ff7f0e"}, "tickfont": {"color": "#ff7f0e"}},
        # Gipuzkoa (right third)
        "xaxis3": {"domain": [0.72, 1.0], "range": x_range, "title": "Year"},
        "yaxis5": {"anchor": "x3", "title": "€/m²", "titlefont": {"color": "#1f77b4"}, "tickfont": {"color": "#1f77b4"}},
        "yaxis6": {"anchor": "x3", "overlaying": "y5", "side": "right",
                   "title": "Foreign %", "titlefont": {"color": "#ff7f0e"}, "tickfont": {"color": "#ff7f0e"}},
        "height": 420,
    }
    spec_q3a = {"data": q3a_traces, "layout": q3a_layout}
    # origin mix per province (latest year)
    origin_year = int(origin["year"].max())
    origin_traces = []
    for prov in ["Araba", "Bizkaia", "Gipuzkoa"]:
        sub = origin[origin["province_std"] == prov].sort_values("pct_of_foreign", ascending=False)
        origin_traces.append(
            {
                "type": "bar",
                "x": sub["nationality"].tolist(),
                "y": sub["pct_of_foreign"].tolist(),
                "name": prov,
            }
        )
    spec_q3b = {
        "data": origin_traces,
        "layout": {
            "title": f"Foreign-origin mix by province — {origin_year}",
            "barmode": "group",
            "yaxis": {"title": "% of foreign population"},
            "legend": {"orientation": "h", "y": -0.2},
        },
    }

    # --- Q4 charts
    spec_q4 = {
        "data": [
            trace_line(poverty, "year", "poverty_rate_pv",        "Poverty-risk rate (%)"),
            trace_line(poverty, "year", "foreign_population_pct", "Foreign population (%)", yaxis="y2"),
        ],
        "layout": {
            "title": "Poverty-risk rate vs foreign-population share — Basque Country",
            "xaxis": {"title": "Year"},
            "yaxis":  {"title": "Poverty rate (%)"},
            "yaxis2": {"title": "Foreign population (%)", "overlaying": "y", "side": "right"},
            "hovermode": "x unified",
            "legend": {"orientation": "h", "y": -0.2},
        },
    }

    # --- Q5 charts
    # Filter to actual Basque Parliament election years only.
    # The source contains mid-term composition snapshots (e.g. 1982, 1985)
    # that reflect parliamentary group changes, not election results.
    _election_years = {1980, 1984, 1986, 1990, 1994, 1998, 2001, 2005, 2009, 2012, 2016, 2020, 2024}
    elec = elec[elec["year"].isin(_election_years)].copy()

    # top parties only: keep those that ever exceeded 5 seats
    top_parties = (
        elec.groupby("party")["seats"].max().sort_values(ascending=False)
        .head(8).index.tolist()
    )
    traces = []
    for p in top_parties:
        sub = elec[elec["party"] == p].sort_values("year")
        traces.append({
            "type": "bar",
            "x": sub["year"].tolist(),
            "y": sub["seats"].tolist(),
            "name": p,
        })
    spec_q5 = {
        "data": traces,
        "layout": {
            "title": "Basque Parliament seats by party",
            "barmode": "stack",
            "xaxis": {"title": "Election year", "type": "category"},
            "yaxis":  {"title": "Seats"},
            "legend": {"orientation": "h", "y": -0.3},
        },
    }
    # migration rates line chart
    mig_spec = {
        "data": [
            trace_line(migration, "year", "in_migration_rate_total",   "Total"),
            trace_line(migration, "year", "in_migration_rate_spanish", "Spanish nationals"),
            trace_line(migration, "year", "in_migration_rate_foreign", "Foreign residents"),
        ],
        "layout": {
            "title": "Inter-regional in-migration rate (per 1 000 residents) — Basque Country",
            "xaxis": {"title": "Year"},
            "yaxis": {"title": "Rate per 1 000"},
            "hovermode": "x unified",
            "legend": {"orientation": "h", "y": -0.2},
        },
    }

    # --- headline numbers for the cards
    kpi = {
        "crime_latest_year":      int(crime["year"].max()),
        "crime_foreign_pop_pct":  float(crime.iloc[-1]["foreign_pct_of_population"]),
        "crime_foreign_det_pct":  float(crime.iloc[-1]["foreign_pct_of_detentions"]),
        "crime_over_rep_ratio":   float(crime.iloc[-1]["over_representation_ratio"]),
        "unemp_latest_year": latest_year_labour,
        "unemp_spanish": float(
            labour[(labour["year"] == latest_year_labour) & (labour["nationality"] == "Spanish")]
            ["unemployment_rate"].iloc[0]
        ),
        "unemp_foreign": float(
            labour[(labour["year"] == latest_year_labour) & (labour["nationality"] == "Foreign")]
            ["unemployment_rate"].iloc[0]
        ),
        "housing_latest_year": int(housing["year"].max()),
        "housing_bizkaia_price": float(
            housing[(housing["year"] == housing["year"].max()) & (housing["province_std"] == "Bizkaia")]
            ["price_per_m2"].iloc[0]
        ),
        "poverty_latest_year": int(poverty["year"].max()),
        "poverty_latest_rate": float(poverty.iloc[-1]["poverty_rate_pv"]),
        "foreign_pct_latest": float(foreign_pct_basque().iloc[-1]["foreign_population_pct"]),
        "foreign_pct_year":   int(foreign_pct_basque().iloc[-1]["year"]),
        "elec_latest_year":   int(elec["year"].max()),
    }

    # --- HTML shell
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Immigration in the Basque Country — Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  * {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    margin: 0; padding: 0;
    background: #f7f8fa; color: #1a1d24;
  }}
  header {{
    background: linear-gradient(135deg, #1e3a5f, #2d5a87);
    color: white; padding: 30px 40px;
  }}
  header h1 {{ margin: 0 0 8px 0; font-size: 26px; font-weight: 600; }}
  header p  {{ margin: 0; opacity: 0.85; font-size: 14px; }}
  nav {{
    display: flex; background: white; border-bottom: 1px solid #e0e4ea;
    padding: 0 40px; position: sticky; top: 0; z-index: 10;
  }}
  nav button {{
    background: none; border: none; padding: 18px 22px; cursor: pointer;
    font-size: 14px; font-weight: 500; color: #5a6578;
    border-bottom: 3px solid transparent; transition: all 0.15s;
  }}
  nav button:hover {{ color: #1e3a5f; }}
  nav button.active {{ color: #1e3a5f; border-bottom-color: #1e3a5f; }}
  main {{ padding: 30px 40px; max-width: 1400px; margin: 0 auto; }}
  .tab {{ display: none; }}
  .tab.active {{ display: block; }}
  .tab h2 {{ margin: 0 0 8px 0; font-size: 22px; }}
  .tab .subtitle {{ color: #6b7588; margin-bottom: 24px; }}
  .kpis {{ display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }}
  .kpi {{
    background: white; border: 1px solid #e0e4ea; border-radius: 8px;
    padding: 16px 20px; flex: 1; min-width: 180px;
  }}
  .kpi .label {{ font-size: 12px; color: #6b7588; text-transform: uppercase; letter-spacing: 0.5px; }}
  .kpi .value {{ font-size: 24px; font-weight: 600; color: #1e3a5f; margin-top: 4px; }}
  .kpi .note  {{ font-size: 12px; color: #8892a3; margin-top: 2px; }}
  .chart {{
    background: white; border: 1px solid #e0e4ea; border-radius: 8px;
    padding: 16px; margin-bottom: 20px; height: 480px;
  }}
  .chart-pair {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
  @media (max-width: 1100px) {{ .chart-pair {{ grid-template-columns: 1fr; }} }}
  footer {{
    text-align: center; padding: 24px; color: #8892a3; font-size: 12px;
    border-top: 1px solid #e0e4ea; background: white; margin-top: 40px;
  }}
</style>
</head>
<body>

<header>
  <h1>Immigration in the Basque Country</h1>
  <p>Interactive dashboard built from the 5 dbt marts · Data: INE, EUSTAT, Basque Parliament</p>
</header>

<nav>
  <button class="tab-btn active" data-tab="q1">Q1 · Crime</button>
  <button class="tab-btn" data-tab="q2">Q2 · Labour</button>
  <button class="tab-btn" data-tab="q3">Q3 · Housing</button>
  <button class="tab-btn" data-tab="q4">Q4 · Poverty</button>
  <button class="tab-btn" data-tab="q5">Q5 · Elections</button>
</nav>

<main>

  <section id="q1" class="tab active">
    <h2>Q1 · Are foreigners over-represented in detentions?</h2>
    <p class="subtitle">Compares the foreign share of detention events against the foreign
       share of the resident population. Source counts events (not unique people) and includes
       non-resident foreign nationals in the numerator, so the over-representation <em>ratio</em>
       is the defensible metric — raw per-100k rates would be an upper bound.</p>
    <div class="kpis">
      <div class="kpi"><div class="label">Year</div><div class="value">{kpi['crime_latest_year']}</div></div>
      <div class="kpi"><div class="label">Foreign share of population</div><div class="value">{kpi['crime_foreign_pop_pct']:.1f}%</div><div class="note">registered residents</div></div>
      <div class="kpi"><div class="label">Foreign share of detentions</div><div class="value">{kpi['crime_foreign_det_pct']:.1f}%</div><div class="note">all detention events</div></div>
      <div class="kpi"><div class="label">Over-representation</div><div class="value">{kpi['crime_over_rep_ratio']:.1f}×</div><div class="note">share ratio</div></div>
    </div>
    {chart("fig_q1", spec_q1)}
  </section>

  <section id="q2" class="tab">
    <h2>Q2 · Are foreign workers more exposed to unemployment?</h2>
    <p class="subtitle">Basque unemployment rates by nationality plus the national sectoral mix
       that explains the gap.</p>
    <div class="kpis">
      <div class="kpi"><div class="label">Year</div><div class="value">{kpi['unemp_latest_year']}</div></div>
      <div class="kpi"><div class="label">Spanish unemployment</div><div class="value">{kpi['unemp_spanish']:.1f}%</div></div>
      <div class="kpi"><div class="label">Foreign unemployment</div><div class="value">{kpi['unemp_foreign']:.1f}%</div></div>
      <div class="kpi"><div class="label">Gap</div><div class="value">{kpi['unemp_foreign']-kpi['unemp_spanish']:+.1f} pp</div></div>
    </div>
    <div class="chart-pair">
      {chart("fig_q2a", spec_q2a)}
      {chart("fig_q2b", spec_q2b)}
    </div>
  </section>

  <section id="q3" class="tab">
    <h2>Q3 · Does immigration track housing-price evolution?</h2>
    <p class="subtitle">For each province: average price per m² (left axis) plotted against
       the foreign-population share (right axis), with the 2024 origin breakdown underneath.</p>
    <div class="kpis">
      <div class="kpi"><div class="label">Year</div><div class="value">{kpi['housing_latest_year']}</div></div>
      <div class="kpi"><div class="label">Bizkaia price</div><div class="value">{kpi['housing_bizkaia_price']:.0f} €/m²</div></div>
      <div class="kpi"><div class="label">PV foreign share</div><div class="value">{kpi['foreign_pct_latest']:.1f}%</div><div class="note">as of {kpi['foreign_pct_year']}</div></div>
    </div>
    {chart("fig_q3a", spec_q3a)}
    {chart("fig_q3b", spec_q3b)}
  </section>

  <section id="q4" class="tab">
    <h2>Q4 · Does the rise in foreigners track poverty risk?</h2>
    <p class="subtitle">Basque poverty-risk rate (INE, base 2013) against the foreign-population
       share of the Basque Country.</p>
    <div class="kpis">
      <div class="kpi"><div class="label">Year</div><div class="value">{kpi['poverty_latest_year']}</div></div>
      <div class="kpi"><div class="label">Poverty-risk rate</div><div class="value">{kpi['poverty_latest_rate']:.1f}%</div></div>
      <div class="kpi"><div class="label">Foreign share</div><div class="value">{kpi['foreign_pct_latest']:.1f}%</div></div>
    </div>
    {chart("fig_q4", spec_q4)}
  </section>

  <section id="q5" class="tab">
    <h2>Q5 · Do electoral results shift with demographic change?</h2>
    <p class="subtitle">Seats per party in every Basque Parliament election, with the
       foreign-population share overlaid on the right axis and inter-regional mobility
       shown below.</p>
    <div class="kpis">
      <div class="kpi"><div class="label">Last election</div><div class="value">{kpi['elec_latest_year']}</div></div>
      <div class="kpi"><div class="label">Foreign share then</div><div class="value">{kpi['foreign_pct_latest']:.1f}%</div></div>
    </div>
    {chart("fig_q5", spec_q5)}
    {chart("fig_q5b", mig_spec)}
  </section>

</main>

<footer>
  Generated from <code>build_dashboard.py</code> · Data sources: INE · EUSTAT · Basque Parliament ·
  Mart logic mirrors the dbt project in <code>models/</code>.
</footer>

<script>
  document.querySelectorAll('.tab-btn').forEach(btn => {{
    btn.addEventListener('click', () => {{
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab').forEach(s => s.classList.remove('active'));
      btn.classList.add('active');
      var tab = document.getElementById(btn.dataset.tab);
      tab.classList.add('active');
      // Plotly charts rendered in hidden tabs have zero width.
      // After making the tab visible, resize every chart inside it.
      tab.querySelectorAll('.chart').forEach(c => {{
        if (c.data) Plotly.Plots.resize(c);
      }});
    }});
  }});
</script>

</body>
</html>
"""
    return html


if __name__ == "__main__":
    html = build_html()
    OUT.write_text(html, encoding="utf-8")
    print(f"Dashboard written to: {OUT}")
    print(f"Size: {OUT.stat().st_size/1024:.1f} KB")
