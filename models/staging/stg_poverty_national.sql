SELECT territory, year, value AS poverty_rate
FROM {{ source('bigdatamarina', 'ine_poverty_stats') }}
WHERE level = 'ccaa'
  AND territory = 'Total Nacional'
  AND indicator = 'Todas las edades. Tasa de riesgo de pobreza (renta del año anterior a la entrevista). Base 2013.'
ORDER BY year
