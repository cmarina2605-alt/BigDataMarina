{#
    Business-facing view for Research Question 2:
    "Are foreign workers more exposed to unemployment or economically inactive
     status than Spanish nationals?"

    Combines, at País Vasco level:
      * EUSTAT rates by nationality          — activity / employment / unemployment
        (from stg_activity_rate_annual, stg_employment_rate_annual,
         stg_unemployment_rate_annual);
      * EUSTAT absolute volumes by nationality — employed / unemployed / inactive /
        total population (from stg_eustat_activity_by_nationality);
      * National sectoral mix by nationality   — what % of each nationality's
        Spanish-level employment sits in Agriculture / Industry / Construction /
        Services (from stg_ine_employment_sector_national);
      * National wage benchmark                — annual and hourly gross earnings
        per worker (from stg_epa_earnings_national).

    The sectoral mix and wage benchmark are national, not Basque: they provide
    the macro backdrop against which the Basque-specific rates and volumes
    should be read (the gap in unemployment is partly explained by the
    concentration of foreign workers in construction and services, and the
    wage context frames how costly inactivity is).

    Output granularity: one row per (year, nationality) with nationality ∈
    {'Spanish', 'Foreign'}.  'Total' is deliberately excluded so the mart
    directly answers the RQ2 comparison.
#}
WITH rates AS (
    SELECT
        a.year,
        'Spanish' AS nationality,
        a.spanish_nationality AS activity_rate,
        u.spanish_nationality AS unemployment_rate,
        e.spanish_nationality AS employment_rate
    FROM {{ ref('stg_activity_rate_annual')     }} a
    JOIN {{ ref('stg_unemployment_rate_annual') }} u USING (year, province_std)
    JOIN {{ ref('stg_employment_rate_annual')   }} e USING (year, province_std)
    WHERE a.province_std = 'País Vasco'

    UNION ALL

    SELECT
        a.year,
        'Foreign' AS nationality,
        a.foreign_nationality AS activity_rate,
        u.foreign_nationality AS unemployment_rate,
        e.foreign_nationality AS employment_rate
    FROM {{ ref('stg_activity_rate_annual')     }} a
    JOIN {{ ref('stg_unemployment_rate_annual') }} u USING (year, province_std)
    JOIN {{ ref('stg_employment_rate_annual')   }} e USING (year, province_std)
    WHERE a.province_std = 'País Vasco'
),

volumes_raw AS (
    -- Aggregate continent-level nationalities into Foreign and Total.
    -- The source has: Africa, America, Asia, Europe, Oceania, Total.
    -- 'Total' includes all nationalities (Spanish + Foreign).
    SELECT
        year,
        CASE WHEN nationality = 'Total' THEN 'Total' ELSE 'Foreign' END AS nationality,
        SUM(employed_population)   AS employed_population,
        SUM(unemployed_population) AS unemployed_population,
        SUM(inactive_population)   AS inactive_population,
        SUM(total_population)      AS total_population
    FROM {{ ref('stg_eustat_activity_by_nationality') }}
    WHERE province_std IN ('Araba', 'Bizkaia', 'Gipuzkoa')
    GROUP BY year, CASE WHEN nationality = 'Total' THEN 'Total' ELSE 'Foreign' END
),

volumes AS (
    -- Foreign volumes come directly from the aggregation above.
    SELECT year, nationality, employed_population, unemployed_population,
           inactive_population, total_population
    FROM volumes_raw
    WHERE nationality = 'Foreign'

    UNION ALL

    -- Spanish volumes = Total minus Foreign for each year.
    SELECT
        t.year,
        'Spanish' AS nationality,
        t.employed_population   - f.employed_population   AS employed_population,
        t.unemployed_population - f.unemployed_population AS unemployed_population,
        t.inactive_population   - f.inactive_population   AS inactive_population,
        t.total_population      - f.total_population      AS total_population
    FROM volumes_raw t
    JOIN volumes_raw f ON t.year = f.year AND f.nationality = 'Foreign'
    WHERE t.nationality = 'Total'
),

sector_mix AS (
    -- National sectoral distribution of employment by nationality:
    -- what share of each nationality's employed workforce sits in each sector.
    SELECT
        year,
        nationality_bucket AS nationality,
        ROUND(SAFE_DIVIDE(
            MAX(CASE WHEN sector = 'Agricultura'  THEN employed_thousands END),
            MAX(CASE WHEN sector = 'Total'        THEN employed_thousands END)) * 100, 2)
            AS national_pct_agriculture,
        ROUND(SAFE_DIVIDE(
            MAX(CASE WHEN sector = 'Industria'    THEN employed_thousands END),
            MAX(CASE WHEN sector = 'Total'        THEN employed_thousands END)) * 100, 2)
            AS national_pct_industry,
        ROUND(SAFE_DIVIDE(
            MAX(CASE WHEN sector = 'Construcción' THEN employed_thousands END),
            MAX(CASE WHEN sector = 'Total'        THEN employed_thousands END)) * 100, 2)
            AS national_pct_construction,
        ROUND(SAFE_DIVIDE(
            MAX(CASE WHEN sector = 'Servicios'    THEN employed_thousands END),
            MAX(CASE WHEN sector = 'Total'        THEN employed_thousands END)) * 100, 2)
            AS national_pct_services
    FROM {{ ref('stg_ine_employment_sector_national') }}
    WHERE nationality_bucket IN ('Spanish', 'Foreign')
    GROUP BY year, nationality_bucket
),

earnings AS (
    -- National wage backdrop: same number for every nationality in a given year.
    SELECT
        year,
        gross_earnings_per_worker_eur AS national_gross_earnings_per_worker_eur,
        gross_earnings_per_hour_eur   AS national_gross_earnings_per_hour_eur
    FROM {{ ref('stg_epa_earnings_national') }}
)

SELECT
    r.year,
    r.nationality,
    r.activity_rate,
    r.employment_rate,
    r.unemployment_rate,
    v.employed_population,
    v.unemployed_population,
    v.inactive_population,
    v.total_population,
    ROUND(SAFE_DIVIDE(v.inactive_population,   v.total_population) * 100, 2) AS inactive_share_pct,
    ROUND(SAFE_DIVIDE(v.unemployed_population, v.total_population) * 100, 2) AS unemployed_share_pct,
    s.national_pct_agriculture,
    s.national_pct_industry,
    s.national_pct_construction,
    s.national_pct_services,
    w.national_gross_earnings_per_worker_eur,
    w.national_gross_earnings_per_hour_eur
FROM rates r
LEFT JOIN volumes    v ON v.year = r.year AND v.nationality = r.nationality
LEFT JOIN sector_mix s ON s.year = r.year AND s.nationality = r.nationality
LEFT JOIN earnings   w ON w.year = r.year
ORDER BY r.year, r.nationality
