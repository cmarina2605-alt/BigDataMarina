SELECT
    territory AS province,
    CASE
        WHEN LOWER(territory) LIKE 'araba%'    THEN 'Araba'
        WHEN LOWER(territory) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(territory) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
    END AS province_std,
    year,
    CAST(ROUND(population) AS INT64) AS total_population
FROM {{ source('bigdatamarina', 'population_total') }}
WHERE level = 'province'
  AND territory IN ('Araba/Álava', 'Bizkaia', 'Gipuzkoa')
  AND sex = 'Ambos sexos'
  AND age_group = 'Todas las edades'
  AND periodo = '1 de julio de'
ORDER BY province_std, year
