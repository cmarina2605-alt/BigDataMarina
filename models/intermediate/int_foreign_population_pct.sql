{#
    Combines three foreign-population sources into a single continuous series
    by province and year, using chain-linking to ensure a smooth transition
    between statistical series that use different methodologies.

    Priority chain:
      1. EUSTAT nationality data (preferred, covers 2010, 2015-2024)
      2. INE Padrón Continuo (annual 1998-2022, fills EUSTAT gaps)
      3. INE ECP (2021-2026, fills years beyond EUSTAT / Padrón)

    Chain-linking methodology:
      In years where both the primary series (EUSTAT + Padrón) and the ECP
      overlap, a per-province ratio is computed:
          chain_ratio = AVG(primary_pct / ecp_pct)  over overlap years
      For ECP-only years (those not covered by EUSTAT or Padrón), the ECP
      values are scaled by this ratio so the series is continuous with no
      level jump at the source-transition boundary.
#}
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

-- Combined primary series (EUSTAT + Padrón, no gaps between 1998-2024)
primary_series AS (
    SELECT * FROM eustat
    UNION ALL
    SELECT * FROM padron
),

-- Raw ECP data for ALL its years (needed for overlap computation)
ecp_raw AS (
    SELECT
        e.province_std, e.year,
        e.foreign_population,
        e.total_population,
        CAST(e.foreign_population_pct AS NUMERIC) AS foreign_population_pct
    FROM {{ ref('stg_ine_ecp_foreign_province') }} e
),

-- Chain-link ratio per province: primary / ECP in overlap years
chain_ratio AS (
    SELECT
        p.province_std,
        AVG(p.foreign_population_pct / NULLIF(e.foreign_population_pct, 0)) AS ratio
    FROM primary_series p
    JOIN ecp_raw e
        ON p.province_std = e.province_std AND p.year = e.year
    GROUP BY p.province_std
),

-- ECP-only years (not covered by EUSTAT or Padrón), scaled by chain ratio
ecp_chain_linked AS (
    SELECT
        e.province_std,
        e.year,
        CAST(ROUND(e.foreign_population * COALESCE(cr.ratio, 1)) AS INT64) AS foreign_population,
        e.total_population,
        ROUND(e.foreign_population_pct * COALESCE(cr.ratio, 1), 2) AS foreign_population_pct
    FROM ecp_raw e
    LEFT JOIN chain_ratio cr ON e.province_std = cr.province_std
    WHERE NOT EXISTS (
        SELECT 1 FROM primary_series ps
        WHERE ps.province_std = e.province_std AND ps.year = e.year
    )
)

SELECT * FROM primary_series
UNION ALL
SELECT * FROM ecp_chain_linked
ORDER BY province_std, year
