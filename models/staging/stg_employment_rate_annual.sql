SELECT
    year,
    CASE
        WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
        WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
        WHEN LOWER(province) LIKE '%euskadi%'
          OR REGEXP_CONTAINS(province, r'(?i)pa[ií]s vasco') THEN 'País Vasco'
    END AS province_std,
    ROUND(AVG(CASE WHEN nationality = 'Spanish nationality' THEN value END), 1) AS spanish_nationality,
    ROUND(AVG(CASE WHEN nationality = 'Foreign nationality' THEN value END), 1) AS foreign_nationality,
    ROUND(AVG(CASE WHEN nationality = 'Total'               THEN value END), 1) AS total
FROM {{ source('bigdatamarina', 'eustat_activity_nationality') }}
WHERE quarter = 'Annual average'
  AND rate_type = 'Employment rate'
GROUP BY year, province_std
ORDER BY year, province_std
