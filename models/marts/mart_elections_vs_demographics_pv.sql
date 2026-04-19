{#
    Business-facing view for Research Question 5:
    "How do electoral outcomes in the Basque Parliament shift as demographics
     change over time?"

    For every Basque Parliament election year, the view exposes:
      * seats and seat-share obtained by each party (main dependent variable);
      * the foreign-population share of the Basque Country that same year
        (from int_population_pais_vasco_year);
      * inter-regional in-migration rates from stg_migration_basque_country —
        a second demographic lens capturing mobility *within* Spain
        (total / Spanish / foreign rates per 1 000 inhabitants).

    Together these give two complementary explanatory variables: the stock of
    foreign residents and the flow of people moving in from other Spanish
    regions.  That lets RQ5 distinguish whether electoral shifts correlate
    with immigration or with general demographic churn.

    Party names are normalised (upper-case, trimmed) so small typographical
    variations across election years collapse to a single political force.
#}
WITH elections AS (
    SELECT
        CAST(year AS INT64)     AS year,
        UPPER(TRIM(party_name)) AS party_name,
        SUM(total_seats)        AS seats
    FROM {{ ref('stg_elections_results') }}
    GROUP BY year, party_name
),

totals AS (
    SELECT
        year,
        SUM(seats) AS total_seats_year
    FROM elections
    GROUP BY year
),

demographics AS (
    SELECT
        year,
        total_population,
        foreign_population,
        foreign_population_pct
    FROM {{ ref('int_population_pais_vasco_year') }}
),

migration AS (
    SELECT
        year,
        in_migration_rate_total,
        in_migration_rate_spanish,
        in_migration_rate_foreign
    FROM {{ ref('stg_migration_basque_country') }}
)

SELECT
    e.year,
    e.party_name,
    e.seats,
    t.total_seats_year,
    ROUND(SAFE_DIVIDE(e.seats, t.total_seats_year) * 100, 2) AS seats_share_pct,
    d.total_population,
    d.foreign_population,
    d.foreign_population_pct,
    m.in_migration_rate_total,
    m.in_migration_rate_spanish,
    m.in_migration_rate_foreign
FROM elections e
JOIN          totals       t ON t.year = e.year
LEFT JOIN     demographics d ON d.year = e.year
LEFT JOIN     migration    m ON m.year = e.year
ORDER BY e.year, e.seats DESC
