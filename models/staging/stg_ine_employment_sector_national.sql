{#
    Staging view for the INE employment-by-sector table
    (base table: ine_employment).

    The source is national ("Total Nacional") and breaks down the number of
    employed persons (relation_activity = 'Ocupados', thousands of people)
    by economic sector (Agricultura / Industria / Construcción / Servicios /
    Total) and by detailed nationality bucket.

    Important: the nationality column mixes *leaves* and *aggregates*.  The
    four 'Extranjera: <region>' rows (Unión Europea / América Latina /
    Resto de Europa / Resto del mundo y apátrida) are the disjoint leaves;
    'Extranjera: Total' is their aggregate, and the unqualified 'Total' is
    the all-nationalities aggregate.  To avoid double-counting, the staging
    projection:
      * treats Española + Doble nacionalidad as 'Spanish';
      * treats ONLY the four disjoint 'Extranjera: <region>' leaves as
        'Foreign' (explicitly excluding 'Extranjera: Total');
      * passes the unqualified 'Total' row straight through as 'Total'.

    The view then sums across the leaves inside each bucket, so every row
    is additive.  Output: one row per (year, sector, nationality_bucket).

    This view feeds mart_labour_market_nationality_pv as the *national
    sectoral mix* benchmark, which is the lens that explains *why* the
    Basque-specific unemployment gap exists (foreigners are concentrated in
    construction and services, Spanish nationals more diversified).
#}
SELECT
    CAST(year AS INT64) AS year,
    sector,
    CASE
        WHEN nationality = 'Total'                                 THEN 'Total'
        WHEN nationality IN ('Española', 'Doble nacionalidad')     THEN 'Spanish'
        WHEN nationality IN ('Extranjera: Unión Europea',
                             'Extranjera: América Latina',
                             'Extranjera: Resto de Europa',
                             'Extranjera: Resto del mundo y apátrida')
                                                                   THEN 'Foreign'
        ELSE 'Other'
    END AS nationality_bucket,
    ROUND(SUM(value), 1) AS employed_thousands
FROM {{ source('bigdatamarina', 'ine_employment') }}
WHERE relation_activity = 'Ocupados'
  AND territory         = 'Total Nacional'
  AND sex               = 'Ambos sexos'
  AND sector           IN ('Agricultura', 'Industria', 'Construcción', 'Servicios', 'Total')
  AND nationality      != 'Extranjera: Total'
GROUP BY year, sector, nationality_bucket
ORDER BY year, sector, nationality_bucket
