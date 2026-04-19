SELECT
    CASE
        WHEN LOWER(province) LIKE 'araba%'    THEN 'Araba'
        WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
    END AS province_std,
    region,
    province AS province_original,
    year, nationality,
    SUM(value) AS total_detentions
FROM {{ source('bigdatamarina', 'crime_detentions') }}
WHERE sex = 'Ambos sexos'
  AND (
        LOWER(province) LIKE 'araba%'
     OR LOWER(province) LIKE 'bizkaia%'
     OR LOWER(province) LIKE 'gipuzkoa%'
      )
GROUP BY province_std, region, province, year, nationality
ORDER BY year, province_std, nationality
