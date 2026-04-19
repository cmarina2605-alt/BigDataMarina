SELECT
    province,
    province_std,
    year,
    CAST(COALESCE(SUM(CASE
        WHEN nationality NOT IN ('Total', 'España', 'Spanish nationality', 'Nacionalidad española')
        THEN population ELSE 0
    END), 0) AS INT64) AS foreign_population
FROM (
    SELECT
        province,
        CASE
            WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
            WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
            WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
        END AS province_std,
        year, nationality, population
    FROM {{ source('bigdatamarina', 'population_eustat_nationality') }}
    WHERE relation_with_activity = 'Total'
      AND sex = 'Total'
) sub
GROUP BY province, province_std, year
ORDER BY province_std, year
