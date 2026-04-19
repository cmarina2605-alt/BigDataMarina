SELECT
    ccaa,
    CASE
        WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
        WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
    END AS province_std,
    nationality,
    year,
    SUM(CASE WHEN relation_with_activity = 'Employed population'   THEN population ELSE 0 END) AS employed_population,
    SUM(CASE WHEN relation_with_activity = 'Unemployed population' THEN population ELSE 0 END) AS unemployed_population,
    SUM(CASE WHEN relation_with_activity = 'Inactive population'   THEN population ELSE 0 END) AS inactive_population,
    SUM(CASE WHEN relation_with_activity = 'Total'                 THEN population ELSE 0 END) AS total_population
FROM {{ source('bigdatamarina', 'population_eustat_nationality') }}
WHERE sex = 'Total'
  AND province IN ('Araba/Alava', 'Bizkaia', 'Gipuzkoa')
GROUP BY ccaa, province_std, nationality, year
ORDER BY province_std, year
