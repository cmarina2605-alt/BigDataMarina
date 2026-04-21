-- INE Estadística Continua de Población — foreign population by province (2021+).
-- This source extends the foreign-population series beyond the
-- discontinued Padrón Continuo (which stopped in 2022).
-- Note: ECP uses a broader estimation methodology and is NOT
-- directly comparable to Padrón figures for the same year.
SELECT
    province_std,
    year,
    foreign_population,
    total_population,
    foreign_population_pct
FROM {{ source('bigdatamarina', 'ine_ecp_foreign') }}
WHERE province_std IN ('Araba', 'Bizkaia', 'Gipuzkoa')
ORDER BY province_std, year
