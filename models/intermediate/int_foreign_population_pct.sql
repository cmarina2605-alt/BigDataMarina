SELECT
    f.province_std, f.year,
    f.foreign_population,
    i.total_population,
    ROUND(CAST(f.foreign_population AS NUMERIC) / NULLIF(i.total_population, 0) * 100, 2) AS foreign_population_pct
FROM {{ ref('stg_eustat_foreign_population_province') }} f
JOIN {{ ref('stg_ine_population_province') }} i
    ON f.province_std = i.province_std AND f.year = i.year
ORDER BY f.province_std, f.year
