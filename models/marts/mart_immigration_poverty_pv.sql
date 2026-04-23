SELECT
    p.year,
    p.poverty_rate,
    n.poverty_rate AS national_poverty_rate,
    ROUND(n.poverty_rate - p.poverty_rate, 1) AS poverty_gap_pp,
    pop.total_population,
    pop.foreign_population,
    pop.foreign_population_pct
FROM {{ ref('stg_poverty_basque_country') }} p
LEFT JOIN {{ ref('stg_poverty_national') }} n ON p.year = n.year
LEFT JOIN {{ ref('int_population_pais_vasco_year') }} pop ON p.year = pop.year
ORDER BY p.year
