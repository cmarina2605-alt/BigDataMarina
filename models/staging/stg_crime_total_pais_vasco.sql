SELECT
    territory AS province, year,
    SUM(CASE WHEN sex = 'Ambos sexos' THEN value ELSE 0 END) AS ambos_sexos,
    SUM(CASE WHEN sex = 'Masculino'   THEN value ELSE 0 END) AS hombres,
    SUM(CASE WHEN sex = 'Femenino'    THEN value ELSE 0 END) AS mujeres
FROM {{ source('bigdatamarina', 'crime_detentions_total') }}
WHERE crime_type = 'TOTAL INFRACCIONES PENALES'
  AND age_group = 'TOTAL edad'
  AND territory = 'PAÍS VASCO'
GROUP BY territory, year
ORDER BY territory, year
