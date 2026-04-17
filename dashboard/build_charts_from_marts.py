"""
Dashboard chart builder.

This script queries the three dbt marts directly from the Supabase
PostgreSQL database and renders four matplotlib figures that answer
the research questions of the Assignment 2 dashboard:

    1. Foreign population share by province        -> stg_eustat_foreign_population_province
    2. Detention rate per 100k by nationality      -> mart_immigration_crime_study_pv_rates
    3. Foreign population share vs. poverty risk   -> mart_immigration_poverty_pv
    4. Housing price vs. foreign share (BI/GI)     -> mart_housing_foreign_share

The script is the serving-layer entry point for the analytical use case:
it does NOT read any pre-materialised CSV.  All aggregates are computed
by dbt in the warehouse; here we only SELECT the finished mart rows
and plot them.

Usage:
    python dashboard/build_charts_from_marts.py

Output:
    dashboard/images/chart_foreign_share.png
    dashboard/images/chart_crime_rates.png
    dashboard/images/chart_poverty.png
    dashboard/images/chart_housing.png
"""

import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
IMG_DIR = BASE_DIR / "dashboard" / "images"
IMG_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv(BASE_DIR / ".env")


# Visual identity (matches report/slides)
NAVY = "#1F3A6B"
BLUE = "#2E5597"
DARK_RED = "#8B2E2E"
GOLD = "#C9A24A"
GREEN = "#3F8A5A"

plt.rcParams.update({
    "font.family": "serif",
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.titleweight": "bold",
    "axes.titlecolor": NAVY,
    "axes.labelcolor": NAVY,
    "axes.edgecolor": "#888",
    "xtick.color": "#333",
    "ytick.color": "#333",
    "axes.grid": True,
    "grid.color": "#DDD",
    "grid.linestyle": "--",
    "grid.linewidth": 0.6,
    "figure.facecolor": "white",
})


# ------------------------------------------------------------
# Warehouse helpers
# ------------------------------------------------------------

def get_conn():
    """Open a PostgreSQL connection to the Supabase warehouse."""
    return psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
    )


def query(sql: str) -> pd.DataFrame:
    """Run a SELECT against the warehouse and return a DataFrame."""
    with get_conn() as conn:
        return pd.read_sql(sql, conn)


# ------------------------------------------------------------
# Chart 1 — Foreign share by province
# ------------------------------------------------------------

def chart_foreign_share():
    sql = """
        SELECT year, province, foreign_population_pct
        FROM stg_eustat_foreign_population_province
        WHERE year BETWEEN 2015 AND 2022
        ORDER BY province, year
    """
    df = query(sql)

    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    palette = {"Araba": DARK_RED, "Bizkaia": BLUE, "Gipuzkoa": GREEN}
    for province, grp in df.groupby("province"):
        ax.plot(
            grp["year"], grp["foreign_population_pct"],
            marker="o", linewidth=2,
            color=palette.get(province, NAVY), label=province,
        )
    ax.set_title("Foreign population share by Basque province (2015–2022)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Foreign share (%)")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(IMG_DIR / "chart_foreign_share.png", dpi=200)
    plt.close(fig)


# ------------------------------------------------------------
# Chart 2 — Detention rate per 100k by nationality
# ------------------------------------------------------------

def chart_crime_rates():
    sql = """
        SELECT year,
               foreign_crime_rate_per_100k,
               spanish_crime_rate_per_100k
        FROM mart_immigration_crime_study_pv_rates
        ORDER BY year
    """
    df = query(sql)

    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    ax.plot(df["year"], df["foreign_crime_rate_per_100k"],
            marker="o", linewidth=2, color=DARK_RED, label="Foreign")
    ax.plot(df["year"], df["spanish_crime_rate_per_100k"],
            marker="s", linewidth=2, color=BLUE, label="Spanish")
    ax.set_title("Detention rate per 100k by nationality — Basque Country")
    ax.set_xlabel("Year")
    ax.set_ylabel("Detentions per 100 000 residents")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(IMG_DIR / "chart_crime_rates.png", dpi=200)
    plt.close(fig)


# ------------------------------------------------------------
# Chart 3 — Foreign share vs poverty risk (dual axis)
# ------------------------------------------------------------

def chart_poverty():
    sql = """
        SELECT year, foreign_population_pct, poverty_rate
        FROM mart_immigration_poverty_pv
        ORDER BY year
    """
    df = query(sql)

    fig, ax1 = plt.subplots(figsize=(7.5, 4.2))
    ax1.plot(df["year"], df["foreign_population_pct"],
             marker="o", linewidth=2, color=BLUE, label="Foreign share (%)")
    ax1.set_xlabel("Year")
    ax1.set_ylabel("Foreign share (%)", color=BLUE)
    ax1.tick_params(axis="y", labelcolor=BLUE)

    ax2 = ax1.twinx()
    ax2.plot(df["year"], df["poverty_rate"],
             marker="s", linewidth=2, color=DARK_RED, label="Poverty rate (%)")
    ax2.set_ylabel("Poverty rate (%)", color=DARK_RED)
    ax2.tick_params(axis="y", labelcolor=DARK_RED)
    ax2.grid(False)

    ax1.set_title("Foreign population share vs. poverty risk — Basque Country")
    fig.tight_layout()
    fig.savefig(IMG_DIR / "chart_poverty.png", dpi=200)
    plt.close(fig)


# ------------------------------------------------------------
# Chart 4 — Housing price vs foreign share (Bizkaia + Gipuzkoa)
# ------------------------------------------------------------

def chart_housing():
    sql = """
        SELECT year, province, housing_price, foreign_population_pct
        FROM mart_housing_foreign_share
        WHERE province IN ('Bizkaia', 'Gipuzkoa')
        ORDER BY province, year
    """
    df = query(sql)

    fig, axes = plt.subplots(1, 2, figsize=(9.5, 4.2), sharey=False)
    for ax, province in zip(axes, ["Bizkaia", "Gipuzkoa"]):
        sub = df[df["province"] == province]
        ax.plot(sub["year"], sub["housing_price"],
                marker="o", linewidth=2, color=BLUE, label="Housing price (€/m²)")
        ax2 = ax.twinx()
        ax2.plot(sub["year"], sub["foreign_population_pct"],
                 marker="s", linewidth=2, color=GOLD, label="Foreign share (%)")
        ax2.grid(False)
        ax.set_title(province)
        ax.set_xlabel("Year")
        ax.set_ylabel("Housing price (€/m²)", color=BLUE)
        ax2.set_ylabel("Foreign share (%)", color=GOLD)

    fig.suptitle("Housing price vs. foreign share — Bizkaia & Gipuzkoa",
                 color=NAVY, fontweight="bold")
    fig.tight_layout()
    fig.savefig(IMG_DIR / "chart_housing.png", dpi=200)
    plt.close(fig)


# ------------------------------------------------------------
# Entry point
# ------------------------------------------------------------

def main():
    print("Connecting to Supabase and rendering dashboard charts...")
    chart_foreign_share()
    print("  OK  chart_foreign_share.png")
    chart_crime_rates()
    print("  OK  chart_crime_rates.png")
    chart_poverty()
    print("  OK  chart_poverty.png")
    chart_housing()
    print("  OK  chart_housing.png")
    print(f"Done. Charts written to {IMG_DIR}")


if __name__ == "__main__":
    main()
