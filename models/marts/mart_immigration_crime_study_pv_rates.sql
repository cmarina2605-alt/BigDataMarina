{#
    Mart for RQ1 — Are foreigners over-represented in detentions in PV?

    Methodological note: the underlying source `crime_detentions` is INE's
    "Estadística de personas detenidas e investigadas".  It counts
    person-EVENTS (a person detained / investigated multiple times in a
    year contributes multiple rows) and includes non-resident persons
    (tourists, irregular migrants) in the numerator.  The resident
    population denominator only counts registered residents (padrón).
    Because numerator and denominator refer to different universes, the
    raw "events per 100 000 residents" rate is an upper bound and is NOT
    directly interpretable as "share of group detained per year".

    The defensible, scale-invariant metric is the
    over-representation ratio:

        over_representation_ratio
            = (foreign share of detention events)
            / (foreign share of resident population)

    Ratio > 1 means foreigners are over-represented in detention events
    relative to their share of the resident population; ratio = 1 means
    proportional representation; ratio < 1 means under-represented.
    This metric uses the same numerator framing on both sides of the
    fraction, so the events-vs-people issue cancels out as long as
    Spanish nationals are not systematically multi-counted at a
    different rate than foreigners.

    The per-100k rates are kept for completeness and labelled as
    "event rates" so a reader knows not to interpret them as "share of
    group detained".
#}
SELECT
    t.year,
    p.total_population,
    p.foreign_population,
    (p.total_population - p.foreign_population)                                                                     AS spanish_population,
    t.ambos_sexos                                                                                                   AS total_detention_events,
    f.total_foreign_detentions                                                                                      AS foreign_detention_events,
    (t.ambos_sexos - f.total_foreign_detentions)                                                                    AS spanish_detention_events,
    p.foreign_population_pct,
    ROUND(CAST(f.total_foreign_detentions AS NUMERIC) / NULLIF(t.ambos_sexos, 0) * 100, 2)                          AS foreign_detention_pct,
    ROUND(
        (CAST(f.total_foreign_detentions AS NUMERIC) / NULLIF(t.ambos_sexos, 0))
      / NULLIF(CAST(p.foreign_population AS NUMERIC) / NULLIF(p.total_population, 0), 0),
        2
    )                                                                                                               AS over_representation_ratio,
    ROUND(CAST(t.ambos_sexos AS NUMERIC) / NULLIF(p.total_population, 0) * 100000, 2)                               AS total_event_rate_per_100k,
    ROUND(CAST(f.total_foreign_detentions AS NUMERIC) / NULLIF(p.foreign_population, 0) * 100000, 2)                AS foreign_event_rate_per_100k,
    ROUND(CAST((t.ambos_sexos - f.total_foreign_detentions) AS NUMERIC)
        / NULLIF((p.total_population - p.foreign_population), 0) * 100000, 2)                                       AS spanish_event_rate_per_100k
FROM {{ ref('stg_crime_total_pais_vasco') }} t
LEFT JOIN {{ ref('int_foreign_detentions_pv') }} f      ON t.year = f.year
LEFT JOIN {{ ref('int_population_pais_vasco_year') }} p ON t.year = p.year
ORDER BY t.year
