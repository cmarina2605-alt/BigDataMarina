-- Combines three foreign-population sources into a single continuous series
-- by province and year, using a priority chain:
--   1. EUSTAT nationality data (preferred, but sparse: 2010, 2015-2024)
--   2. INE Padrón Continuo (annual 1998-2022, fills EUSTAT gaps)
--   3. INE ECP (2021-2025, fills years beyond Padrón discontinuation)
--
-- Chain-linking: ECP uses a different methodology than Padrón, producing
-- higher foreign-population figures for the same year. To avoid a visible
-- scale discontinuity we compute a per-province ratio in the overlap years
-- (where both Padrón and ECP report data) and apply it to ECP-only years,
-- rescaling ECP figures to the Padrón scale.
--
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

-- Raw ECP and Padrón data (no exclusions) for computing the chain-link ratio
padron_raw AS (
    SELECT
        p.province_std, p.year,
        p.foreign_population AS padron_foreign_pop,
        i.total_population   AS padron_total_pop,
        ROUND(CAST(p.foreign_population AS NUMERIC) / NULLIF(i.total_population, 0) * 100, 2) AS padron_pct
    FROM {{ ref('stg_ine_padron_foreign_province') }} p
    JOIN {{ ref('stg_ine_population_province') }} i
        ON p.province_std = i.province_std AND p.year = i.year
),

ecp_raw AS (
    SELECT
        province_std, year,
        foreign_population AS ecp_foreign_pop,
        total_population   AS ecp_total_pop,
        CAST(foreign_population_pct AS NUMERIC) AS ecp_pct
    FROM {{ ref('stg_ine_ecp_foreign_province') }}
    WHERE province_std IN ('Araba', 'Bizkaia', 'Gipuzkoa')
),

-- Per-province scaling ratio from overlap years (both Padrón and ECP exist)
-- Only scale foreign population — total population is not affected by the
-- Padrón-to-ECP methodology change.
chain_link_ratio AS (
    SELECT
        p.province_std,
        AVG(SAFE_DIVIDE(CAST(p.padron_foreign_pop AS NUMERIC), CAST(e.ecp_foreign_pop AS NUMERIC))) AS pop_ratio
    FROM padron_raw p
    JOIN ecp_raw e ON p.province_std = e.province_std AND p.year = e.year
    GROUP BY p.province_std
),

-- ECP data for years not covered by EUSTAT or Padrón, chain-linked to Padrón scale
ecp_chain_linked AS (
    SELECT
        e.province_std,
        e.year,
        CAST(ROUND(e.ecp_foreign_pop * COALESCE(r.pop_ratio, 1)) AS INT64)  AS foreign_population,
        e.ecp_total_pop                                                      AS total_population,
        ROUND(SAFE_DIVIDE(e.ecp_foreign_pop * COALESCE(r.pop_ratio, 1), e.ecp_total_pop) * 100, 2) AS foreign_population_pct
    FROM ecp_raw e
    LEFT JOIN chain_link_ratio r ON e.province_std = r.province_std
    WHERE NOT EXISTS (
        -- Only exclude if EUSTAT + INE population actually produces a complete row
        SELECT 1 FROM {{ ref('stg_eustat_foreign_population_province') }} eu
        JOIN {{ ref('stg_ine_population_province') }} ip
            ON eu.province_std = ip.province_std AND eu.year = ip.year
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
SELECT * FROM ecp_chain_linked
ORDER BY province_std, year
