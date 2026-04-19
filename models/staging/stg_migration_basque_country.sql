{#
    Staging view for the INE interregional-migration table
    (base table: migration_birth_country).

    The source publishes, for every autonomous community, the rate of
    inflows from other Spanish regions per 1 000 inhabitants
    ("Tasa de Inmigración Interautonómica").  This is a second demographic
    lens on immigration: it captures *internal* mobility (people moving
    between Spanish regions), independently of foreign-origin immigration.

    The staging projection:
      * filters to País Vasco at the headline slice
        (Ambos sexos, Total edades, Anual, 'Dato base');
      * pivots the nationality column into three side-by-side metrics so
        that every year is a single row carrying the Ambas /
        Española / Extranjera rates.

    This view feeds mart_elections_vs_demographics_pv: together with the
    foreign-population share it explains whether demographic movement in
    the Basque Country is driven mainly by foreign arrivals or by
    Spanish nationals moving from other regions.
#}
SELECT
    CAST(year AS INT64) AS year,
    ROUND(MAX(CASE WHEN nationality = 'Ambas nacionalidades' THEN value END), 2)
        AS in_migration_rate_total,
    ROUND(MAX(CASE WHEN nationality = 'Española'             THEN value END), 2)
        AS in_migration_rate_spanish,
    ROUND(MAX(CASE WHEN nationality = 'Extranjera'           THEN value END), 2)
        AS in_migration_rate_foreign
FROM {{ source('bigdatamarina', 'migration_birth_country') }}
WHERE region      = 'País Vasco'
  AND indicator   = 'Tasa de Inmigración Interautonómica'
  AND sex         = 'Ambos sexos'
  AND age_group   = 'Total edades'
  AND periodicity = 'Anual'
  AND data_type   = 'Dato base'
GROUP BY year
ORDER BY year
