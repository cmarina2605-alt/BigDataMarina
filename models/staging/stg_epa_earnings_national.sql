{#
    Staging view for the INE EPA earnings table
    (base table: epa_contract_stats).

    The source is national and exposes two earnings indicators:
      * 'Ganancia (bruta) por trabajador y año' — annual gross earnings
        per worker (EUR);
      * 'Ganancia (bruta) por hora y año'       — hourly gross earnings
        (EUR/hour).

    The staging projection pivots both indicators into two columns per year,
    filtered to the headline slice (Ambos sexos, Total age-group, Total
    disability), so that one row per year carries both figures.  This feeds
    the labour mart as a **national wage benchmark**: the gap between
    Basque foreign and Spanish unemployment rates should be read against
    the backdrop of how national wages are evolving.
#}
SELECT
    CAST(year AS INT64) AS year,
    ROUND(MAX(CASE WHEN indicator = 'Ganancia (bruta) por trabajador y año'
                   THEN value END), 2) AS gross_earnings_per_worker_eur,
    ROUND(MAX(CASE WHEN indicator = 'Ganancia (bruta) por hora y año'
                   THEN value END), 2) AS gross_earnings_per_hour_eur
FROM {{ source('bigdatamarina', 'epa_contract_stats') }}
WHERE sex        = 'Ambos sexos'
  AND age_group  = 'Total'
  AND disability = 'Total'
GROUP BY year
ORDER BY year
