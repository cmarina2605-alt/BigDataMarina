-- INE Padrón Continuo — foreign population by province (1998-2022).
-- This source fills the year-gaps left by the EUSTAT nationality
-- breakdown (which only covers 2010, 2015-2024).
SELECT
    province,
    province_std,
    year,
    foreign_population
FROM {{ source('bigdatamarina', 'ine_padron_foreign') }}
WHERE province_std IN ('Araba', 'Bizkaia', 'Gipuzkoa')
ORDER BY province_std, year
