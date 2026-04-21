-- Combines three foreign-population sources into a single continuous series
-- by province and year, using a priority chain:
--   1. EUSTAT nationality data (preferred, but sparse: 2010, 2015-2024)
--   2. INE Padrón Continuo (annual 1998-2022, fills EUSTAT gaps)
--   3. INE ECP (2021-2025, fills years beyond Padrón discontinuation)
-- Each source only contributes rows not already covered by a higher-priority one.
WITH eustat AS (
    SELECT
        f.province_std, f.year,
        f.foreign_population,
        i.total_population,
        ROUND(CAST(f.foreign_population AS NUMERIC) / NULLIF(i.total_population, 0) * 100, 2) AS foreign_population_pct
    FROM {{ ref('stg_eustat_foreign_population_province') }} f
    JOIN {{ ref('stg_ine_population_province') }} i
        ON f.province_std = i.province_std AND f.year = i.year
),
padron AS (
    SELECT
        p.province_std, p.year,
        p.foreign_population,
        i.total_population,
        ROUND(CAST(p.foreign_population AS NUMERIC) / NULLIF(i.total_population, 0) * 100, 2) AS foreign_population_pct
    FROM {{ ref('stg_ine_padron_foreign_province') }} p
    JOIN {{ ref('stg_ine_population_province') }} i
        ON p.province_std = i.province_std AND p.year = i.year
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ ref('stg_eustat_foreign_population_province') }} e
        WHERE e.province_std = p.province_std AND e.year = p.year
    )
),
ecp AS (
    SELECT
        e.province_std, e.year,
        e.foreign_population,
        e.total_population,
        CAST(e.foreign_population_pct AS NUMERIC) AS foreign_population_pct
    FROM {{ ref('stg_ine_ecp_foreign_province') }} e
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ ref('stg_eustat_foreign_population_province') }} eu
        WHERE eu.province_std = e.province_std AND eu.year = e.year
    )
    AND NOT EXISTS (
        SELECT 1 FROM {{ ref('stg_ine_padron_foreign_province') }} p
        WHERE p.province_std = e.province_std AND p.year = e.year
    )
)
SELECT * FROM eustat
UNION ALL
SELECT * FROM padron
UNION ALL
SELECT * FROM ecp
ORDER BY province_std, year
