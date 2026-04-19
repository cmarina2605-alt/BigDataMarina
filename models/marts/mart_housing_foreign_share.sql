{#
    Business-facing view for Research Question 3:
    "Does immigration concentration vary across provinces, and does it track
     housing price evolution?"

    Layers three signals per (province, year):
      * avg housing price per m2            — from stg_housing_prices_province;
      * foreign-population share             — from int_foreign_population_pct
        (built on stg_ine_population_province +
         stg_eustat_foreign_population_province);
      * dominant foreign origin and its share of foreign residents —
        from stg_eustat_population_by_origin, so the view does not just
        quantify *how many* foreigners live in each province but also
        *where they come from*, which sharpens the RQ3 correlation check
        between housing pressure and the origin mix.

    One row per (province_std, year) covering Araba / Bizkaia / Gipuzkoa.
#}
WITH origin_ranked AS (
    -- Rank every foreign origin within each (province, year) so we can pull
    -- out the dominant origin and its share of the foreign-only population.
    SELECT
        province_std,
        year,
        origin,
        population,
        SUM(population) OVER (PARTITION BY province_std, year) AS foreign_total_pop,
        ROW_NUMBER() OVER (
            PARTITION BY province_std, year
            ORDER BY population DESC
        ) AS origin_rank
    FROM {{ ref('stg_eustat_population_by_origin') }}
    WHERE province_std IN ('Araba', 'Bizkaia', 'Gipuzkoa')
      AND origin_bucket = 'Foreign'
),

top_origin AS (
    SELECT
        province_std,
        year,
        origin                                                               AS top_foreign_origin,
        ROUND(SAFE_DIVIDE(population, foreign_total_pop) * 100, 2)           AS top_foreign_origin_pct
    FROM origin_ranked
    WHERE origin_rank = 1
),

housing AS (
    SELECT
        year,
        province_std,
        avg_price_per_m2
    FROM {{ ref('stg_housing_prices_province') }}
)

SELECT
    h.year,
    h.province_std,
    h.avg_price_per_m2,
    f.foreign_population,
    f.total_population,
    f.foreign_population_pct,
    o.top_foreign_origin,
    o.top_foreign_origin_pct
FROM housing h
JOIN      {{ ref('int_foreign_population_pct') }} f
       ON f.year = h.year
      AND f.province_std = h.province_std
LEFT JOIN top_origin o
       ON o.year = h.year
      AND o.province_std = h.province_std
ORDER BY h.year, h.province_std
