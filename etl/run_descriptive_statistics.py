"""
BIG DATA PROJECT  ANALYTICAL VIEWS CREATION SCRIPT

This script connects to a Supabase PostgreSQL database and creates
 analytical views related to population, immigration, labour market,
crime, poverty, housing and electoral indicators in the Basque Country.

Each view is exported to CSV format for further analysis
(e.g., Power BI or statistical tools).

=====================================================
ANALYTICAL VIEWS CREATED (ORDER OF EXECUTION)
=====================================================

1)  vw_ine_population_province
2)  vw_eustat_population_total_province
3)  vw_eustat_activity_by_nationality
4)  vw_eustat_foreign_population_clean
5)  vw_eustat_foreign_population_province
6)  vw_ine_employment_total_sector
7)  vw_ine_employment_by_sector
8)  vw_crime_total_national
9)  vw_crime_total_pais_vasco
10)vw_crime_by_province_nationality_clean
11) vw_poverty_basque_country
12) vw_poverty_ccaa_all
13) vw_housing_prices_province
14) vw_elections_results
15) vw_foreign_population_pct
16) vw_activity_rate_annual
17) vw_unemployment_rate_annual
18) vw_employment_rate_annual
19) vw_foreign_detentions_pv
20) vw_population_pais_vasco_year
21) vw_immigration_crime_study_pv_rates
22) vw_immigration_poverty_pv
23) vw_housing_foreign_share
"""



import psycopg2
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os


# ======================================================
# PROJECT PATHS
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / "analysis_results"
OUT_DIR.mkdir(exist_ok=True)

load_dotenv(BASE_DIR / ".env")


# ======================================================
# DATABASE CONNECTION
# ======================================================

conn = psycopg2.connect(
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    dbname=os.getenv("DB_NAME"),
    sslmode="require"
)

cur = conn.cursor()

print("Creating analytical views...")
# ======================================================
# DROP
# ======================================================
DROP_ALL = """
DROP VIEW IF EXISTS
    vw_activity_rate_annual,
    vw_employment_rate_annual,
    vw_unemployment_rate_annual,
    vw_foreign_population_pct,
    vw_eustat_foreign_population_clean,
    vw_eustat_population_total_province,
    vw_eustat_foreign_population_province,
    vw_eustat_activity_by_nationality,
    vw_ine_population_province,
    vw_ine_employment_total_sector,
    vw_ine_employment_by_sector,
    vw_crime_total_national,
    vw_crime_total_pais_vasco,
    vw_poverty_basque_country,
    vw_poverty_ccaa_all,
    vw_housing_prices_province,
    vw_elections_results,
    w_foreign_detentions_pv,
    vw_population_pais_vasco_year,
    vw_immigration_crime_study_pv_rates,
    vw_immigration_poverty_pv,
    vw_housing_foreign_share,
    vw_foreign_detentions_pv,
    CASCADE;
"""


cur.execute(DROP_ALL)
conn.commit()

# ======================================================
# 1) INE  TOTAL POPULATION BY PROVINCE
# ======================================================

VIEW_INE = """
CREATE VIEW vw_ine_population_province AS
        SELECT
        territory AS province,

    CASE
        WHEN territory ILIKE 'Araba%' THEN 'Araba'
        WHEN territory ILIKE 'Bizkaia%' THEN 'Bizkaia'
        WHEN territory ILIKE 'Gipuzkoa%' THEN 'Gipuzkoa'
    END AS province_std,

    year,
    ROUND(population)::integer AS total_population

FROM population_total
WHERE level = 'province'
  AND territory IN ('Araba/Álava', 'Bizkaia', 'Gipuzkoa')
  AND sex = 'Ambos sexos'
  AND age_group = 'Todas las edades'
  AND periodo = '1 de julio de'

ORDER BY province_std, year;

"""

cur.execute(VIEW_INE)
conn.commit()

pd.read_sql("SELECT * FROM vw_ine_population_province;", conn)\
  .to_csv(OUT_DIR / "ine_population_province.csv", index=False, encoding="utf-8-sig")

# ======================================================
#2) EUSTAT – TOTAL POPULATION BY PROVINCE
# ======================================================
VIEW_EUSTAT_POPULATION_TOTAL = """

CREATE VIEW vw_eustat_population_total_province AS
SELECT
    province,
    CASE
    WHEN province = 'Araba/Alava' THEN 'Araba'
    WHEN province = 'Bizkaia' THEN 'Bizkaia'
    WHEN province = 'Gipuzkoa' THEN 'Gipuzkoa'
    WHEN province = 'Basque Country' THEN 'Pais Vasco'
    ELSE province
END AS province_std,

    year,
    population::integer AS total_population
FROM population_eustat_nationality
WHERE relation_with_activity = 'Total'
  AND sex = 'Total'
  AND nationality = 'Total'
ORDER BY province_std, year;
"""

cur.execute(VIEW_EUSTAT_POPULATION_TOTAL)
conn.commit()

pd.read_sql("SELECT * FROM vw_eustat_population_total_province;", conn)\
  .to_csv(OUT_DIR / "eustat_population_total_province.CSV", index=False, encoding="utf-8-sig")

# ======================================================
# 3) EUSTAT – ACTIVITY STATUS BY NATIONALITY
# ======================================================

VIEW_EUSTAT_ACTIVITY = """
CREATE VIEW vw_eustat_activity_by_nationality AS
SELECT
    ccaa,

    CASE
        WHEN province = 'Araba/Alava' THEN 'Araba'
        WHEN province = 'Bizkaia' THEN 'Bizkaia'
        WHEN province = 'Gipuzkoa' THEN 'Gipuzkoa'
    END AS province_std,

    nationality,
    year,

    SUM(CASE WHEN relation_with_activity = 'Employed population'
        THEN population ELSE 0 END) AS employed_population,

    SUM(CASE WHEN relation_with_activity = 'Unemployed population'
        THEN population ELSE 0 END) AS unemployed_population,

    SUM(CASE WHEN relation_with_activity = 'Inactive population'
        THEN population ELSE 0 END) AS inactive_population,

    SUM(CASE WHEN relation_with_activity = 'Total'
        THEN population ELSE 0 END) AS total_population

FROM population_eustat_nationality
WHERE sex = 'Total'
  AND province IN ('Araba/Alava','Bizkaia','Gipuzkoa')

GROUP BY
    ccaa,
    province_std,
    nationality,
    year

ORDER BY
    province_std,
    year;


"""

cur.execute(VIEW_EUSTAT_ACTIVITY)
conn.commit()



pd.read_sql("SELECT * FROM vw_eustat_activity_by_nationality;", conn)\
  .to_csv(OUT_DIR / "eustat_activity_by_nationality.csv", index=False, encoding="utf-8-sig")




# ======================================================
# 4) EUSTAT – FOREIGN POPULATION (CLEAN)
# ======================================================

VIEW_EUSTAT_FOREIGN_CLEAN = """

CREATE OR REPLACE VIEW vw_eustat_foreign_population_clean AS
SELECT
    province,

    CASE
        WHEN province ILIKE 'Araba%' THEN 'Araba'
        WHEN province ILIKE 'Bizkaia%' THEN 'Bizkaia'
        WHEN province ILIKE 'Gipuzkoa%' THEN 'Gipuzkoa'
        WHEN province ILIKE '%Basque%' OR province ILIKE '%País Vasco%' OR province ILIKE '%Euskadi%' THEN 'País Vasco'
    END AS province_std,

    year,

    -- Suma SOLO nacionalidades extranjeras
    SUM(
        CASE
            WHEN nationality NOT IN ('Total', 'España', 'Spanish nationality', 'Nacionalidad española')
            THEN population
            ELSE 0
        END
    )::integer AS foreign_population

FROM population_eustat_nationality
WHERE relation_with_activity = 'Total'
  AND sex = 'Total'

GROUP BY province, province_std, year
ORDER BY province_std, year;
"""

cur.execute(VIEW_EUSTAT_FOREIGN_CLEAN)
conn.commit()

pd.read_sql("SELECT * FROM vw_eustat_foreign_population_clean;", conn)\
  .to_csv(OUT_DIR / "eustat_foreign_population_clean.csv", index=False, encoding="utf-8-sig")


# ======================================================
# 5) EUSTAT – FOREIGN POPULATION BY PROVINCE
# ======================================================

VIEW_EUSTAT_FOREIGN = """
CREATE OR REPLACE VIEW vw_eustat_foreign_population_province AS
SELECT
    province,
    province_std,
    year,
    COALESCE(
        SUM(
            CASE
                WHEN nationality NOT IN ('Total', 'España', 'Spanish nationality', 'Nacionalidad española')
                THEN population
                ELSE 0
            END
        ),
        0
    )::integer AS foreign_population
FROM (
    SELECT
        province,
        CASE
            WHEN province ILIKE 'Araba%' THEN 'Araba'
            WHEN province ILIKE 'Bizkaia%' THEN 'Bizkaia'
            WHEN province ILIKE 'Gipuzkoa%' THEN 'Gipuzkoa'

        END AS province_std,
        year,
        nationality,
        population
    FROM population_eustat_nationality
    WHERE relation_with_activity = 'Total'
      AND sex = 'Total'
) sub
GROUP BY province, province_std, year
ORDER BY province_std, year;

"""

cur.execute(VIEW_EUSTAT_FOREIGN)
conn.commit()

pd.read_sql("SELECT * FROM vw_eustat_foreign_population_province;", conn)\
  .to_csv(OUT_DIR / "eustat_foreign_population_province.csv", index=False, encoding="utf-8-sig")


# ======================================================
# 6) INE – EMPLOYMENT (SECTOR = TOTAL)
# ======================================================

VIEW_EMP_TOTAL = """
CREATE VIEW vw_ine_employment_total_sector AS
SELECT
    year,
    territory,
    nationality,
    SUM(CASE WHEN sex = 'Hombres' THEN value ELSE 0 END) AS men,
    SUM(CASE WHEN sex = 'Mujeres' THEN value ELSE 0 END) AS women,
    SUM(CASE WHEN sex = 'Ambos sexos' THEN value ELSE 0 END) AS total
FROM ine_employment
WHERE relation_activity = 'Ocupados'
  AND sector = 'Total'
GROUP BY year, territory, nationality
ORDER BY year, territory, nationality;
"""

cur.execute(VIEW_EMP_TOTAL)
conn.commit()

pd.read_sql("SELECT * FROM vw_ine_employment_total_sector;", conn)\
  .to_csv(OUT_DIR / "ine_employment_total_sector.csv", index=False, encoding="utf-8-sig")


# ======================================================
# 7) INE – EMPLOYMENT BY SECTOR
# ======================================================

VIEW_EMP_SECTOR = """
CREATE VIEW vw_ine_employment_by_sector AS
SELECT
    year,
    territory,
    nationality,
    sector,
    SUM(CASE WHEN sex = 'Hombres' THEN value ELSE 0 END) AS men,
    SUM(CASE WHEN sex = 'Mujeres' THEN value ELSE 0 END) AS women,
    SUM(CASE WHEN sex = 'Ambos sexos' THEN value ELSE 0 END) AS total
FROM ine_employment
WHERE relation_activity = 'Ocupados'
GROUP BY year, territory, nationality, sector
ORDER BY year, territory, nationality, sector;
"""

cur.execute(VIEW_EMP_SECTOR)
conn.commit()

pd.read_sql("SELECT * FROM vw_ine_employment_by_sector;", conn)\
  .to_csv(OUT_DIR / "ine_employment_by_sector.csv", index=False, encoding="utf-8-sig")


# ======================================================
# 8) CRIME – TOTAL INFRACTIONS (NATIONAL)
# ======================================================

VIEW_CRIME_NATIONAL = """
CREATE VIEW vw_crime_total_national AS
SELECT
    territory AS province,
    year,

    SUM(CASE
        WHEN sex = 'Ambos sexos'
        THEN value ELSE 0 END) AS ambos_sexos,

    SUM(CASE
        WHEN sex = 'Masculino'
        THEN value ELSE 0 END) AS hombres,

    SUM(CASE
        WHEN sex = 'Femenino'
        THEN value ELSE 0 END) AS mujeres

FROM crime_detentions_total
WHERE crime_type = 'TOTAL INFRACCIONES PENALES'
  AND age_group = 'TOTAL edad'
GROUP BY territory, year
ORDER BY territory, year;
"""

cur.execute(VIEW_CRIME_NATIONAL)
conn.commit()

pd.read_sql("SELECT * FROM vw_crime_total_national;", conn)\
  .to_csv(OUT_DIR / "crime_total_national.csv", index=False, encoding="utf-8-sig")


# ======================================================
# 9) CRIME – TOTAL INFRACTIONS (PAÍS VASCO)
# ======================================================

VIEW_CRIME_TOTAL_EUSKADI = """
CREATE VIEW vw_crime_total_pais_vasco AS
SELECT
    territory AS province,
    year,

    SUM(CASE
        WHEN sex = 'Ambos sexos'
        THEN value ELSE 0 END) AS ambos_sexos,

    SUM(CASE
        WHEN sex = 'Masculino'
        THEN value ELSE 0 END) AS hombres,

    SUM(CASE
        WHEN sex = 'Femenino'
        THEN value ELSE 0 END) AS mujeres

FROM crime_detentions_total
WHERE crime_type = 'TOTAL INFRACCIONES PENALES'
  AND age_group = 'TOTAL edad'
  AND territory = 'PAÍS VASCO'
GROUP BY territory, year
ORDER BY territory, year;
"""

cur.execute(VIEW_CRIME_TOTAL_EUSKADI)
conn.commit()

pd.read_sql("SELECT * FROM vw_crime_total_pais_vasco;", conn)\
.to_csv(OUT_DIR / "crime_total_pais_vasco.csv", index=False, encoding="utf-8-sig")

# ======================================================
# 10) CRIME – CONTINENT - NACIONALITY (PAÍS VASCO)
# ======================================================

VIEW_CRIME_EUSKADI_NAC = """
CREATE OR REPLACE VIEW vw_crime_by_province_nationality_clean AS
SELECT
    CASE
        WHEN province ILIKE 'Araba%' THEN 'Araba'
        WHEN province ILIKE 'Bizkaia%' THEN 'Bizkaia'
        WHEN province ILIKE 'Gipuzkoa%' THEN 'Gipuzkoa'
    END AS province_std,

    region,
    province AS province_original,
    year,
    nationality,

    SUM(value) AS total_detentions

FROM crime_detentions

WHERE sex = 'Ambos sexos'
  AND (
        province ILIKE 'Araba%'
     OR province ILIKE 'Bizkaia%'
     OR province ILIKE 'Gipuzkoa%'
      )

GROUP BY
    province_std,
    region,
    province,
    year,
    nationality

ORDER BY
    year,
    province_std,
    nationality;
"""

cur.execute(VIEW_CRIME_EUSKADI_NAC)
conn.commit()

pd.read_sql("SELECT * FROM vw_crime_by_province_nationality_clean;", conn)\
.to_csv(OUT_DIR / "vw_crime_by_province_nationality_clean.csv", index=False, encoding="utf-8-sig")


# ======================================================
# 11) INE – POVERTY RATE (PAÍS VASCO)
# ======================================================

VIEW_POVERTY_EUSKADI = """
CREATE VIEW vw_poverty_basque_country AS
SELECT
    territory,
    year,
    value AS poverty_rate
FROM ine_poverty_stats
WHERE level = 'ccaa'
  AND territory = 'País Vasco'
  AND indicator = 'Todas las edades. Tasa de riesgo de pobreza (renta del año anterior a la entrevista). Base 2013.'
ORDER BY year;
"""

cur.execute(VIEW_POVERTY_EUSKADI)
conn.commit()

pd.read_sql("SELECT * FROM vw_poverty_basque_country;", conn)\
  .to_csv(OUT_DIR / "poverty_basque_country.csv", index=False, encoding="utf-8-sig")


# ======================================================
# 12) INE – POVERTY RATE (ALL CCAA)
# ======================================================

VIEW_POVERTY_ALL = """

CREATE VIEW vw_poverty_ccaa_all AS
SELECT
    territory,
    year,
    value AS poverty_rate
FROM ine_poverty_stats
WHERE level = 'ccaa'
  AND indicator = 'Todas las edades. Tasa de riesgo de pobreza (renta del año anterior a la entrevista). Base 2013.'
ORDER BY territory, year;
"""

cur.execute(VIEW_POVERTY_ALL)
conn.commit()

pd.read_sql("SELECT * FROM vw_poverty_ccaa_all;", conn)\
  .to_csv(OUT_DIR / "poverty_ccaa_all.csv", index=False, encoding="utf-8-sig")


# ======================================================
# 13) HOUSING – AVERAGE PRICE PER M2 (BASQUE PROVINCES)
# ======================================================

VIEW_HOUSING = """

CREATE VIEW vw_housing_prices_province AS
SELECT
    province,
    year,
    ROUND(AVG(price_per_m2)::numeric, 2) AS avg_price_per_m2
FROM housing_prices_annual
WHERE province IN ('Araba/Álava', 'Bizkaia', 'Gipuzkoa')
GROUP BY province, year
ORDER BY province, year;
"""

cur.execute(VIEW_HOUSING)
conn.commit()

pd.read_sql("SELECT * FROM vw_housing_prices_province;", conn)\
  .to_csv(OUT_DIR / "housing_prices_province.csv", index=False, encoding="utf-8-sig")

print("View created: vw_housing_prices_province")

# ======================================================
# 14) ELECTIONS – PARLAMENTO VASCO RESULTS
# ======================================================

VIEW_ELECTIONS = """
CREATE VIEW vw_elections_results AS
SELECT
    year,
    party_name,
    SUM(seats) AS total_seats
FROM elections_parlamento_vasco
GROUP BY year, party_name
ORDER BY year, total_seats DESC;
"""

cur.execute(VIEW_ELECTIONS)
conn.commit()

pd.read_sql("SELECT * FROM vw_elections_results;", conn)\
  .to_csv(OUT_DIR / "elections_results.csv", index=False, encoding="utf-8-sig")

print("View created: vw_elections_results")


# ======================================================
# 15) FOREIGN POPULATION PERCENTAGE (INE + EUSTAT)
# ======================================================

VIEW_FOREIGN_PCT = """
CREATE OR REPLACE VIEW vw_foreign_population_pct AS
SELECT
    f.province_std,
    f.year,
    f.foreign_population,
    i.total_population,

    ROUND(
        f.foreign_population::numeric /
        NULLIF(i.total_population,0) * 100,
        2
    ) AS foreign_population_pct

FROM vw_eustat_foreign_population_province f

JOIN vw_ine_population_province i
    ON f.province_std = i.province_std
   AND f.year = i.year

ORDER BY f.province_std, f.year;

"""

cur.execute(VIEW_FOREIGN_PCT)
conn.commit()

pd.read_sql("SELECT * FROM vw_foreign_population_pct;", conn)\
  .to_csv(OUT_DIR / "foreign_population_pct.csv", index=False, encoding="utf-8-sig")

print("View created: vw_foreign_population_pct")

#-- =========================================
#--16) Activity Rate - Annual Average
#-- =========================================

VIEW_FOREIGN_PCT = """
CREATE VIEW vw_activity_rate_annual AS
SELECT
    year,

    CASE
        WHEN province ILIKE 'Araba%' THEN 'Araba'
        WHEN province ILIKE 'Bizkaia%' THEN 'Bizkaia'
        WHEN province ILIKE 'Gipuzkoa%' THEN 'Gipuzkoa'
        WHEN province ILIKE '%Euskadi%'
             OR province ILIKE '%País Vasco%' THEN 'País Vasco'
    END AS province_std,

    ROUND(AVG(value) FILTER (WHERE nationality = 'Spanish nationality')::numeric, 1) AS spanish_nationality,
    ROUND(AVG(value) FILTER (WHERE nationality = 'Foreign nationality')::numeric, 1) AS foreign_nationality,
    ROUND(AVG(value) FILTER (WHERE nationality = 'Total')::numeric, 1) AS total

FROM eustat_activity_nationality
WHERE quarter = 'Annual average'
  AND rate_type = 'Activity rate'
GROUP BY year,
    CASE
        WHEN province ILIKE 'Araba%' THEN 'Araba'
        WHEN province ILIKE 'Bizkaia%' THEN 'Bizkaia'
        WHEN province ILIKE 'Gipuzkoa%' THEN 'Gipuzkoa'
        WHEN province ILIKE '%Euskadi%'
             OR province ILIKE '%País Vasco%' THEN 'País Vasco'
    END
ORDER BY year, province_std;
"""

cur.execute(VIEW_FOREIGN_PCT)
conn.commit()

pd.read_sql("SELECT * FROM vw_activity_rate_annual;", conn)\
  .to_csv(OUT_DIR / "vw_activity_rate_annual.csv", index=False, encoding="utf-8-sig")

print("View created: vw_activity_rate_annual")
#-- =========================================
#-- 17) Unemployment Rate - Annual Average
#-- =========================================

VIEW_FOREIGN_PCT = """
CREATE VIEW vw_unemployment_rate_annual AS
SELECT
    year,

    CASE
        WHEN province ILIKE 'Araba%' THEN 'Araba'
        WHEN province ILIKE 'Bizkaia%' THEN 'Bizkaia'
        WHEN province ILIKE 'Gipuzkoa%' THEN 'Gipuzkoa'
        WHEN province ILIKE '%Euskadi%'
             OR province ILIKE '%País Vasco%' THEN 'País Vasco'
    END AS province_std,

    ROUND(AVG(value) FILTER (WHERE nationality = 'Spanish nationality')::numeric, 1) AS spanish_nationality,
    ROUND(AVG(value) FILTER (WHERE nationality = 'Foreign nationality')::numeric, 1) AS foreign_nationality,
    ROUND(AVG(value) FILTER (WHERE nationality = 'Total')::numeric, 1) AS total

FROM eustat_activity_nationality
WHERE quarter = 'Annual average'
  AND rate_type = 'Unemployment rate'
GROUP BY year,
    CASE
        WHEN province ILIKE 'Araba%' THEN 'Araba'
        WHEN province ILIKE 'Bizkaia%' THEN 'Bizkaia'
        WHEN province ILIKE 'Gipuzkoa%' THEN 'Gipuzkoa'
        WHEN province ILIKE '%Euskadi%'
             OR province ILIKE '%País Vasco%' THEN 'País Vasco'
    END
ORDER BY year, province_std;
"""

cur.execute(VIEW_FOREIGN_PCT)
conn.commit()

pd.read_sql("SELECT * FROM vw_unemployment_rate_annual;", conn)\
  .to_csv(OUT_DIR / "vw_unemployment_rate_annual.csv", index=False, encoding="utf-8-sig")

print("View created: vw_unemployment_rate_annual")
#-- =========================================
#--  18)  Employment Rate - Annual Average
#-- =========================================

VIEW_FOREIGN_PCT = """
CREATE VIEW vw_employment_rate_annual AS
SELECT
    year,

    CASE
        WHEN province ILIKE 'Araba%' THEN 'Araba'
        WHEN province ILIKE 'Bizkaia%' THEN 'Bizkaia'
        WHEN province ILIKE 'Gipuzkoa%' THEN 'Gipuzkoa'
        WHEN province ILIKE '%Euskadi%'
             OR province ILIKE '%País Vasco%' THEN 'País Vasco'
    END AS province_std,

    ROUND(AVG(value) FILTER (WHERE nationality = 'Spanish nationality')::numeric, 1) AS spanish_nationality,
    ROUND(AVG(value) FILTER (WHERE nationality = 'Foreign nationality')::numeric, 1) AS foreign_nationality,
    ROUND(AVG(value) FILTER (WHERE nationality = 'Total')::numeric, 1) AS total

FROM eustat_activity_nationality
WHERE quarter = 'Annual average'
  AND rate_type = 'Employment rate'
GROUP BY year,
    CASE
        WHEN province ILIKE 'Araba%' THEN 'Araba'
        WHEN province ILIKE 'Bizkaia%' THEN 'Bizkaia'
        WHEN province ILIKE 'Gipuzkoa%' THEN 'Gipuzkoa'
        WHEN province ILIKE '%Euskadi%'
             OR province ILIKE '%País Vasco%' THEN 'País Vasco'
    END
ORDER BY year, province_std;
"""

cur.execute(VIEW_FOREIGN_PCT)
conn.commit()

pd.read_sql("SELECT * FROM vw_employment_rate_annual;", conn)\
  .to_csv(OUT_DIR / "vw_employment_rate_annual.csv", index=False, encoding="utf-8-sig")

print("View created: vw_employment_rate_annual")

#-- =========================================
#--  19)  FOREIGN DETENTIONS – PAÍS VASCO (AGGREGATED)
#-- =========================================

VIEW_FOREIGN_DETENTIONS = """
CREATE VIEW vw_foreign_detentions_pv AS
SELECT
    year,
    SUM(total_detentions) AS total_foreign_detentions
FROM vw_crime_by_province_nationality_clean
GROUP BY year
ORDER BY year;

"""

cur.execute(VIEW_FOREIGN_DETENTIONS)
conn.commit()

pd.read_sql("SELECT * FROM vw_foreign_detentions_pv;", conn)\
  .to_csv(OUT_DIR / "foreign_detentions_pv.csv", index=False, encoding="utf-8-sig")

print("View created: vw_employment_rate_annual")
#-- =========================================
#--  20) POPULATION – PAÍS VASCO (AGGREGATED FROM 3 PROVINCES)
#-- =========================================

VIEW_POPULATION_AGGREGATED = """
CREATE VIEW vw_population_pais_vasco_year AS
SELECT
    year,
    SUM(total_population)::bigint AS total_population,
    SUM(foreign_population)::bigint AS foreign_population,
    ROUND(
        SUM(foreign_population)::numeric /
        NULLIF(SUM(total_population),0) * 100,
        2
    ) AS foreign_population_pct
FROM vw_foreign_population_pct
WHERE province_std IN ('Araba','Bizkaia','Gipuzkoa')
GROUP BY year
ORDER BY year;


"""

cur.execute(VIEW_POPULATION_AGGREGATED)
conn.commit()

pd.read_sql("SELECT * FROM vw_population_pais_vasco_year;", conn)\
  .to_csv(OUT_DIR / "vw_population_pais_vasco_year.csv", index=False, encoding="utf-8-sig")

print("View created: vw_population_pais_vasco_year")
#-- =========================================
#-- 21) IMMIGRATION vs CRIME STUDY – PAÍS VASCO (FINAL MODEL)
#-- =========================================

VIEW_FOREIGN_DETENTIONS = """
CREATE VIEW vw_immigration_crime_study_pv_rates AS
SELECT
    t.year,

    -- Población
    p.total_population,
    p.foreign_population,
    (p.total_population - p.foreign_population) AS spanish_population,

    -- Detenciones
    t.ambos_sexos AS total_detentions,
    f.total_foreign_detentions,
    (t.ambos_sexos - f.total_foreign_detentions) AS spanish_detentions,

    -- % demográfico
    p.foreign_population_pct,

    -- % detenciones extranjeras
    ROUND(
        f.total_foreign_detentions::numeric /
        NULLIF(t.ambos_sexos,0) * 100,
        2
    ) AS foreign_detention_pct,

    -- Tasa total por 100.000
    ROUND(
        t.ambos_sexos::numeric /
        NULLIF(p.total_population,0) * 100000,
        2
    ) AS total_crime_rate_per_100k,

    -- Tasa extranjera por 100.000
    ROUND(
        f.total_foreign_detentions::numeric /
        NULLIF(p.foreign_population,0) * 100000,
        2
    ) AS foreign_crime_rate_per_100k,

    -- Tasa española por 100.000
    ROUND(
        (t.ambos_sexos - f.total_foreign_detentions)::numeric /
        NULLIF((p.total_population - p.foreign_population),0) * 100000,
        2
    ) AS spanish_crime_rate_per_100k

FROM vw_crime_total_pais_vasco t

JOIN vw_foreign_detentions_pv f
    ON t.year = f.year

JOIN vw_population_pais_vasco_year p
    ON t.year = p.year

ORDER BY t.year;


"""

cur.execute(VIEW_FOREIGN_DETENTIONS)
conn.commit()

pd.read_sql("SELECT * FROM vw_immigration_crime_study_pv_rates;", conn)\
  .to_csv(OUT_DIR / "immigration_crime_study_pv_rates.csv", index=False, encoding="utf-8-sig")

print("View created: vw_immigration_crime_study_pv_rates")
#-- =========================================
#-- 22) IMMIGRATION AND POVERTY – BASQUE COUNTRY
#-- =========================================

VIEW_IMMIGRATION_POVERTY = """
CREATE VIEW vw_immigration_poverty_pv AS
SELECT
    p.year,
    p.poverty_rate,

    pop.total_population,
    pop.foreign_population,
    pop.foreign_population_pct

FROM vw_poverty_basque_country p
JOIN vw_population_pais_vasco_year pop
    ON p.year = pop.year

ORDER BY p.year;
"""
cur.execute(VIEW_IMMIGRATION_POVERTY)
conn.commit()

pd.read_sql("SELECT * FROM vw_immigration_poverty_pv;", conn)\
  .to_csv(OUT_DIR / "immigration_poverty_pv.csv", index=False, encoding="utf-8-sig")

print("View created: vw_immigration_poverty_pv")
#-- =========================================
#-- 23) housing foreign share
#-- =========================================

VIEW_HOUSING = """
CREATE OR REPLACE VIEW vw_housing_foreign_share AS
SELECT
    sub.year,
    sub.province_std,
    ROUND(AVG(sub.price_per_m2)::numeric, 2) AS avg_price_per_m2,
    f.foreign_population,
    f.total_population,
    f.foreign_population_pct
FROM (
    SELECT
        year,
        price_per_m2,
        CASE
            WHEN province ILIKE 'Araba%' THEN 'Araba'
            WHEN province ILIKE 'Bizkaia%' THEN 'Bizkaia'
            WHEN province ILIKE 'Gipuzkoa%' THEN 'Gipuzkoa'
        END AS province_std
    FROM housing_prices_annual
    WHERE province IN ('Araba/Álava','Bizkaia','Gipuzkoa')
) sub
JOIN vw_foreign_population_pct f
    ON sub.year = f.year
   AND sub.province_std = f.province_std
GROUP BY
    sub.year,
    sub.province_std,
    f.foreign_population,
    f.total_population,
    f.foreign_population_pct
ORDER BY
    sub.year,
    sub.province_std;


"""
cur.execute(VIEW_HOUSING)
conn.commit()

pd.read_sql("SELECT * FROM vw_housing_foreign_share;", conn)\
  .to_csv(OUT_DIR / "vw_housing_foreign_share.csv", index=False, encoding="utf-8-sig")

print("View created: vw_housing_foreign_share")
# ======================================================
# ======================================================
cur.close()
conn.close()

print("All analytical views created successfully.")
