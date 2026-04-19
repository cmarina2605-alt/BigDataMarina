{#
    Intermediate view: Basque Country foreign-population breakdown by
    origin region and year.

    Derives directly from stg_eustat_population_by_origin, summing the
    three provinces (Araba + Bizkaia + Gipuzkoa) so the result sits at
    País Vasco level.  One row per (year, origin); the origin keeps its
    EUSTAT label (e.g. "América del Sur", "Paises del Magreb",
    "Europa del Este") plus a convenience `origin_bucket` column
    ('Foreign' / 'España' / 'Total') for quick filtering.

    Downstream consumers:
      * mart_housing_foreign_share uses the Foreign slice to rank origin
        regions per province and show whether price growth correlates
        with one particular origin or with overall foreign share.
#}
SELECT
    year,
    origin,
    origin_bucket,
    CAST(SUM(population) AS INT64) AS population_basque
FROM {{ ref('stg_eustat_population_by_origin') }}
WHERE province_std IN ('Araba', 'Bizkaia', 'Gipuzkoa')
GROUP BY year, origin, origin_bucket
ORDER BY year, origin
