SELECT year, SUM(total_detentions) AS total_foreign_detentions
FROM {{ ref('stg_crime_by_province_nationality_clean') }}
WHERE nationality NOT IN ('España', 'Española', 'Total', 'Sin determinar')
GROUP BY year
ORDER BY year
