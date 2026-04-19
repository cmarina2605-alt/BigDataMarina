SELECT
    year,
    CAST(SUM(total_population)   AS INT64) AS total_population,
    CAST(SUM(foreign_population) AS INT64) AS foreign_population,
    ROUND(
        CAST(SUM(foreign_population) AS NUMERIC) /
        NULLIF(CAST(SUM(total_population) AS NUMERIC), 0) * 100,
        2
    ) AS foreign_population_pct
FROM {{ ref('int_foreign_population_pct') }}
WHERE province_std IN ('Araba', 'Bizkaia', 'Gipuzkoa')
GROUP BY year
ORDER BY year
