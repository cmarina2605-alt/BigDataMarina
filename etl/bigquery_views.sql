-- ============================================================
-- BIGQUERY VIEWS - BigDataMarina Project
-- Replace YOUR_PROJECT.YOUR_DATASET with your actual values
-- Run in order (later views depend on earlier ones)
-- ============================================================

-- ============================================================
-- 1) vw_ine_population_province
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_ine_population_province` AS
SELECT
    territory AS province,
    CASE
        WHEN LOWER(territory) LIKE 'araba%'    THEN 'Araba'
        WHEN LOWER(territory) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(territory) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
    END AS province_std,
    year,
    CAST(ROUND(population) AS INT64) AS total_population
FROM `YOUR_PROJECT.YOUR_DATASET.population_total`
WHERE level = 'province'
  AND territory IN ('Araba/Álava', 'Bizkaia', 'Gipuzkoa')
  AND sex = 'Ambos sexos'
  AND age_group = 'Todas las edades'
  AND periodo = '1 de julio de'
ORDER BY province_std, year;

-- ============================================================
-- 2) vw_eustat_population_total_province
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_eustat_population_total_province` AS
SELECT
    province,
    CASE
        WHEN province = 'Araba/Alava'    THEN 'Araba'
        WHEN province = 'Bizkaia'        THEN 'Bizkaia'
        WHEN province = 'Gipuzkoa'       THEN 'Gipuzkoa'
        WHEN province = 'Basque Country' THEN 'Pais Vasco'
        ELSE province
    END AS province_std,
    year,
    CAST(population AS INT64) AS total_population
FROM `YOUR_PROJECT.YOUR_DATASET.population_eustat_nationality`
WHERE relation_with_activity = 'Total'
  AND sex = 'Total'
  AND nationality = 'Total'
ORDER BY province_std, year;

-- ============================================================
-- 3) vw_eustat_activity_by_nationality
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_eustat_activity_by_nationality` AS
SELECT
    ccaa,
    CASE
        WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
        WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
    END AS province_std,
    nationality,
    year,
    SUM(CASE WHEN relation_with_activity = 'Employed population'   THEN population ELSE 0 END) AS employed_population,
    SUM(CASE WHEN relation_with_activity = 'Unemployed population' THEN population ELSE 0 END) AS unemployed_population,
    SUM(CASE WHEN relation_with_activity = 'Inactive population'   THEN population ELSE 0 END) AS inactive_population,
    SUM(CASE WHEN relation_with_activity = 'Total'                 THEN population ELSE 0 END) AS total_population
FROM `YOUR_PROJECT.YOUR_DATASET.population_eustat_nationality`
WHERE sex = 'Total'
  AND province IN ('Araba/Alava', 'Bizkaia', 'Gipuzkoa')
GROUP BY ccaa, province_std, nationality, year
ORDER BY province_std, year;

-- ============================================================
-- 4) vw_eustat_foreign_population_clean
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_eustat_foreign_population_clean` AS
SELECT
    province,
    CASE
        WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
        WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
        WHEN LOWER(province) LIKE '%basque%'
          OR REGEXP_CONTAINS(province, r'(?i)pa[ií]s vasco')
          OR LOWER(province) LIKE '%euskadi%' THEN 'País Vasco'
    END AS province_std,
    year,
    CAST(SUM(CASE
        WHEN nationality NOT IN ('Total', 'España', 'Spanish nationality', 'Nacionalidad española')
        THEN population ELSE 0
    END) AS INT64) AS foreign_population
FROM `YOUR_PROJECT.YOUR_DATASET.population_eustat_nationality`
WHERE relation_with_activity = 'Total'
  AND sex = 'Total'
GROUP BY province, province_std, year
ORDER BY province_std, year;

-- ============================================================
-- 5) vw_eustat_foreign_population_province
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_eustat_foreign_population_province` AS
SELECT
    province,
    province_std,
    year,
    CAST(COALESCE(SUM(CASE
        WHEN nationality NOT IN ('Total', 'España', 'Spanish nationality', 'Nacionalidad española')
        THEN population ELSE 0
    END), 0) AS INT64) AS foreign_population
FROM (
    SELECT
        province,
        CASE
            WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
            WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
            WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
        END AS province_std,
        year, nationality, population
    FROM `YOUR_PROJECT.YOUR_DATASET.population_eustat_nationality`
    WHERE relation_with_activity = 'Total'
      AND sex = 'Total'
) sub
GROUP BY province, province_std, year
ORDER BY province_std, year;

-- ============================================================
-- 6) vw_ine_employment_total_sector
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_ine_employment_total_sector` AS
SELECT
    year, territory, nationality,
    SUM(CASE WHEN sex = 'Hombres'     THEN value ELSE 0 END) AS men,
    SUM(CASE WHEN sex = 'Mujeres'     THEN value ELSE 0 END) AS women,
    SUM(CASE WHEN sex = 'Ambos sexos' THEN value ELSE 0 END) AS total
FROM `YOUR_PROJECT.YOUR_DATASET.ine_employment`
WHERE relation_activity = 'Ocupados'
  AND sector = 'Total'
GROUP BY year, territory, nationality
ORDER BY year, territory, nationality;

-- ============================================================
-- 7) vw_ine_employment_by_sector
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_ine_employment_by_sector` AS
SELECT
    year, territory, nationality, sector,
    SUM(CASE WHEN sex = 'Hombres'     THEN value ELSE 0 END) AS men,
    SUM(CASE WHEN sex = 'Mujeres'     THEN value ELSE 0 END) AS women,
    SUM(CASE WHEN sex = 'Ambos sexos' THEN value ELSE 0 END) AS total
FROM `YOUR_PROJECT.YOUR_DATASET.ine_employment`
WHERE relation_activity = 'Ocupados'
GROUP BY year, territory, nationality, sector
ORDER BY year, territory, nationality, sector;

-- ============================================================
-- 8) vw_crime_total_national
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_crime_total_national` AS
SELECT
    territory AS province, year,
    SUM(CASE WHEN sex = 'Ambos sexos' THEN value ELSE 0 END) AS ambos_sexos,
    SUM(CASE WHEN sex = 'Masculino'   THEN value ELSE 0 END) AS hombres,
    SUM(CASE WHEN sex = 'Femenino'    THEN value ELSE 0 END) AS mujeres
FROM `YOUR_PROJECT.YOUR_DATASET.crime_detentions_total`
WHERE crime_type = 'TOTAL INFRACCIONES PENALES'
  AND age_group = 'TOTAL edad'
GROUP BY territory, year
ORDER BY territory, year;

-- ============================================================
-- 9) vw_crime_total_pais_vasco
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_crime_total_pais_vasco` AS
SELECT
    territory AS province, year,
    SUM(CASE WHEN sex = 'Ambos sexos' THEN value ELSE 0 END) AS ambos_sexos,
    SUM(CASE WHEN sex = 'Masculino'   THEN value ELSE 0 END) AS hombres,
    SUM(CASE WHEN sex = 'Femenino'    THEN value ELSE 0 END) AS mujeres
FROM `YOUR_PROJECT.YOUR_DATASET.crime_detentions_total`
WHERE crime_type = 'TOTAL INFRACCIONES PENALES'
  AND age_group = 'TOTAL edad'
  AND territory = 'PAÍS VASCO'
GROUP BY territory, year
ORDER BY territory, year;

-- ============================================================
-- 10) vw_crime_by_province_nationality_clean
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_crime_by_province_nationality_clean` AS
SELECT
    CASE
        WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
        WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
    END AS province_std,
    region,
    province AS province_original,
    year, nationality,
    SUM(value) AS total_detentions
FROM `YOUR_PROJECT.YOUR_DATASET.crime_detentions`
WHERE sex = 'Ambos sexos'
  AND (
        LOWER(province) LIKE 'araba%'
     OR LOWER(province) LIKE 'bizkaia%'
     OR LOWER(province) LIKE 'gipuzkoa%'
      )
GROUP BY province_std, region, province, year, nationality
ORDER BY year, province_std, nationality;

-- ============================================================
-- 11) vw_poverty_basque_country
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_poverty_basque_country` AS
SELECT territory, year, value AS poverty_rate
FROM `YOUR_PROJECT.YOUR_DATASET.ine_poverty_stats`
WHERE level = 'ccaa'
  AND territory = 'País Vasco'
  AND indicator = 'Todas las edades. Tasa de riesgo de pobreza (renta del año anterior a la entrevista). Base 2013.'
ORDER BY year;

-- ============================================================
-- 12) vw_poverty_ccaa_all
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_poverty_ccaa_all` AS
SELECT territory, year, value AS poverty_rate
FROM `YOUR_PROJECT.YOUR_DATASET.ine_poverty_stats`
WHERE level = 'ccaa'
  AND indicator = 'Todas las edades. Tasa de riesgo de pobreza (renta del año anterior a la entrevista). Base 2013.'
ORDER BY territory, year;

-- ============================================================
-- 13) vw_housing_prices_province
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_housing_prices_province` AS
SELECT
    province, year,
    ROUND(AVG(price_per_m2), 2) AS avg_price_per_m2
FROM `YOUR_PROJECT.YOUR_DATASET.housing_prices_annual`
WHERE province IN ('Araba/Álava', 'Bizkaia', 'Gipuzkoa')
GROUP BY province, year
ORDER BY province, year;

-- ============================================================
-- 14) vw_elections_results
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_elections_results` AS
SELECT year, party_name, SUM(seats) AS total_seats
FROM `YOUR_PROJECT.YOUR_DATASET.elections_parlamento_vasco`
GROUP BY year, party_name
ORDER BY year, total_seats DESC;

-- ============================================================
-- 15) vw_foreign_population_pct  (depende de 5 y 1)
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_foreign_population_pct` AS
SELECT
    f.province_std, f.year,
    f.foreign_population,
    i.total_population,
    ROUND(CAST(f.foreign_population AS NUMERIC) / NULLIF(i.total_population, 0) * 100, 2) AS foreign_population_pct
FROM `YOUR_PROJECT.YOUR_DATASET.vw_eustat_foreign_population_province` f
JOIN `YOUR_PROJECT.YOUR_DATASET.vw_ine_population_province` i
    ON f.province_std = i.province_std AND f.year = i.year
ORDER BY f.province_std, f.year;

-- ============================================================
-- 16) vw_activity_rate_annual
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_activity_rate_annual` AS
SELECT
    year,
    CASE
        WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
        WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
        WHEN LOWER(province) LIKE '%euskadi%'
          OR REGEXP_CONTAINS(province, r'(?i)pa[ií]s vasco') THEN 'País Vasco'
    END AS province_std,
    ROUND(AVG(CASE WHEN nationality = 'Spanish nationality' THEN value END), 1) AS spanish_nationality,
    ROUND(AVG(CASE WHEN nationality = 'Foreign nationality' THEN value END), 1) AS foreign_nationality,
    ROUND(AVG(CASE WHEN nationality = 'Total'               THEN value END), 1) AS total
FROM `YOUR_PROJECT.YOUR_DATASET.eustat_activity_nationality`
WHERE quarter = 'Annual average'
  AND rate_type = 'Activity rate'
GROUP BY year, province_std
ORDER BY year, province_std;

-- ============================================================
-- 17) vw_unemployment_rate_annual
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_unemployment_rate_annual` AS
SELECT
    year,
    CASE
        WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
        WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
        WHEN LOWER(province) LIKE '%euskadi%'
          OR REGEXP_CONTAINS(province, r'(?i)pa[ií]s vasco') THEN 'País Vasco'
    END AS province_std,
    ROUND(AVG(CASE WHEN nationality = 'Spanish nationality' THEN value END), 1) AS spanish_nationality,
    ROUND(AVG(CASE WHEN nationality = 'Foreign nationality' THEN value END), 1) AS foreign_nationality,
    ROUND(AVG(CASE WHEN nationality = 'Total'               THEN value END), 1) AS total
FROM `YOUR_PROJECT.YOUR_DATASET.eustat_activity_nationality`
WHERE quarter = 'Annual average'
  AND rate_type = 'Unemployment rate'
GROUP BY year, province_std
ORDER BY year, province_std;

-- ============================================================
-- 18) vw_employment_rate_annual
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_employment_rate_annual` AS
SELECT
    year,
    CASE
        WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
        WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
        WHEN LOWER(province) LIKE '%euskadi%'
          OR REGEXP_CONTAINS(province, r'(?i)pa[ií]s vasco') THEN 'País Vasco'
    END AS province_std,
    ROUND(AVG(CASE WHEN nationality = 'Spanish nationality' THEN value END), 1) AS spanish_nationality,
    ROUND(AVG(CASE WHEN nationality = 'Foreign nationality' THEN value END), 1) AS foreign_nationality,
    ROUND(AVG(CASE WHEN nationality = 'Total'               THEN value END), 1) AS total
FROM `YOUR_PROJECT.YOUR_DATASET.eustat_activity_nationality`
WHERE quarter = 'Annual average'
  AND rate_type = 'Employment rate'
GROUP BY year, province_std
ORDER BY year, province_std;

-- ============================================================
-- 19) vw_foreign_detentions_pv  (depende de 10)
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_foreign_detentions_pv` AS
SELECT year, SUM(total_detentions) AS total_foreign_detentions
FROM `YOUR_PROJECT.YOUR_DATASET.vw_crime_by_province_nationality_clean`
WHERE nationality NOT IN ('España', 'Española', 'Total', 'Sin determinar')
GROUP BY year
ORDER BY year;

-- ============================================================
-- 20) vw_population_pais_vasco_year  (depende de 15)
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_population_pais_vasco_year` AS
SELECT
    year,
    CAST(SUM(total_population)   AS INT64) AS total_population,
    CAST(SUM(foreign_population) AS INT64) AS foreign_population,
    ROUND(
        CAST(SUM(foreign_population) AS NUMERIC) /
        NULLIF(CAST(SUM(total_population) AS NUMERIC), 0) * 100,
        2
    ) AS foreign_population_pct
FROM `YOUR_PROJECT.YOUR_DATASET.vw_foreign_population_pct`
WHERE province_std IN ('Araba', 'Bizkaia', 'Gipuzkoa')
GROUP BY year
ORDER BY year;

-- ============================================================
-- 21) vw_immigration_crime_study_pv_rates  (depende de 9, 19, 20)
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_immigration_crime_study_pv_rates` AS
SELECT
    t.year,
    p.total_population,
    p.foreign_population,
    (p.total_population - p.foreign_population) AS spanish_population,
    t.ambos_sexos AS total_detentions,
    f.total_foreign_detentions,
    (t.ambos_sexos - f.total_foreign_detentions) AS spanish_detentions,
    p.foreign_population_pct,
    ROUND(CAST(f.total_foreign_detentions AS NUMERIC) / NULLIF(t.ambos_sexos, 0) * 100, 2)                                                    AS foreign_detention_pct,
    ROUND(CAST(t.ambos_sexos AS NUMERIC) / NULLIF(p.total_population, 0) * 100000, 2)                                                         AS total_crime_rate_per_100k,
    ROUND(CAST(f.total_foreign_detentions AS NUMERIC) / NULLIF(p.foreign_population, 0) * 100000, 2)                                          AS foreign_crime_rate_per_100k,
    ROUND(CAST((t.ambos_sexos - f.total_foreign_detentions) AS NUMERIC) / NULLIF((p.total_population - p.foreign_population), 0) * 100000, 2) AS spanish_crime_rate_per_100k
FROM `YOUR_PROJECT.YOUR_DATASET.vw_crime_total_pais_vasco` t
JOIN `YOUR_PROJECT.YOUR_DATASET.vw_foreign_detentions_pv` f      ON t.year = f.year
JOIN `YOUR_PROJECT.YOUR_DATASET.vw_population_pais_vasco_year` p ON t.year = p.year
ORDER BY t.year;

-- ============================================================
-- 22) vw_immigration_poverty_pv  (depende de 11, 20)
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_immigration_poverty_pv` AS
SELECT
    p.year, p.poverty_rate,
    pop.total_population, pop.foreign_population, pop.foreign_population_pct
FROM `YOUR_PROJECT.YOUR_DATASET.vw_poverty_basque_country` p
JOIN `YOUR_PROJECT.YOUR_DATASET.vw_population_pais_vasco_year` pop ON p.year = pop.year
ORDER BY p.year;

-- ============================================================
-- 23) vw_housing_foreign_share  (depende de 15)
-- ============================================================
CREATE OR REPLACE VIEW `YOUR_PROJECT.YOUR_DATASET.vw_housing_foreign_share` AS
SELECT
    sub.year, sub.province_std,
    ROUND(AVG(sub.price_per_m2), 2) AS avg_price_per_m2,
    f.foreign_population, f.total_population, f.foreign_population_pct
FROM (
    SELECT
        year, price_per_m2,
        CASE
            WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
            WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
            WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
        END AS province_std
    FROM `YOUR_PROJECT.YOUR_DATASET.housing_prices_annual`
    WHERE province IN ('Araba/Álava', 'Bizkaia', 'Gipuzkoa')
) sub
JOIN `YOUR_PROJECT.YOUR_DATASET.vw_foreign_population_pct` f
    ON sub.year = f.year AND sub.province_std = f.province_std
GROUP BY sub.year, sub.province_std, f.foreign_population, f.total_population, f.foreign_population_pct
ORDER BY sub.year, sub.province_std;
