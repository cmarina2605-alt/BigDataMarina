SELECT
    province AS province_raw,
    CASE
        WHEN LOWER(province) LIKE 'araba%'
          OR LOWER(province) LIKE 'álava%'
          OR LOWER(province) LIKE 'alava%'    THEN 'Araba'
        WHEN LOWER(province) LIKE 'bizkaia%'  THEN 'Bizkaia'
        WHEN LOWER(province) LIKE 'gipuzkoa%' THEN 'Gipuzkoa'
    END AS province_std,
    CAST(year AS INT64) AS year,
    ROUND(AVG(price_per_m2), 2) AS avg_price_per_m2
FROM {{ source('bigdatamarina', 'housing_prices_annual') }}
WHERE LOWER(province) LIKE 'araba%'
   OR LOWER(province) LIKE 'álava%'
   OR LOWER(province) LIKE 'alava%'
   OR LOWER(province) LIKE 'bizkaia%'
   OR LOWER(province) LIKE 'gipuzkoa%'
GROUP BY province_raw, province_std, year
ORDER BY province_std, year
