SELECT
    p.year, p.poverty_rate,
    pop.total_population, pop.foreign_population, pop.foreign_population_pct
FROM {{ ref('stg_poverty_basque_country') }} p
JOIN {{ ref('int_population_pais_vasco_year') }} pop ON p.year = pop.year
ORDER BY p.year
