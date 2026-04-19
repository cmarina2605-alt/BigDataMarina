{#
    Staging view for the EUSTAT total-population table
    (base table: population_eustat_total).

    This table decomposes the population of each Basque province by
    continent / region of birth ("España", "América del Sur",
    "Paises del Magreb", "Europa del Este", etc.).  It is therefore the
    richest source we have for understanding *where* foreign residents come
    from, not just how many there are.

    The staging projection:
      * keeps one row per (province_std, origin, year);
      * standardises the province label to Araba / Bizkaia / Gipuzkoa /
        País Vasco (the source uses "Total" for the Basque Country row);
      * tags every origin as 'España', 'Foreign' or 'Total' so downstream
        views can aggregate cleanly, while also keeping the fine-grained
        `origin` label for the housing mart.
#}
SELECT
    province                                                      AS province_raw,
    CASE
        WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
        WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
        WHEN province = 'Total'               THEN 'País Vasco'
    END                                                           AS province_std,
    nationality                                                   AS origin,
    CASE
        WHEN nationality = 'España'  THEN 'España'
        WHEN nationality = 'Total'   THEN 'Total'
        ELSE 'Foreign'
    END                                                           AS origin_bucket,
    CAST(year AS INT64)                                           AS year,
    CAST(population AS INT64)                                     AS population
FROM {{ source('bigdatamarina', 'population_eustat_total') }}
ORDER BY province_std, year, origin
