{#
    The source contains multiple monthly composition snapshots per year
    (e.g. 1990 has mayo, junio, dic.).  We need exactly ONE consistent
    snapshot per year to avoid double-counting seats.  Strategy: for each
    year, identify the latest month that has data, then keep ONLY the
    parties from that month.  This ensures total seats per year = 75
    (or 60 for 1980).
#}
WITH with_month_num AS (
    SELECT
        year,
        month,
        party_name,
        seats,
        CASE LOWER(TRIM(month))
            WHEN 'enero'  THEN 1  WHEN 'ene.'  THEN 1
            WHEN 'feb.'   THEN 2  WHEN 'febr.' THEN 2
            WHEN 'marzo'  THEN 3
            WHEN 'abril'  THEN 4
            WHEN 'mayo'   THEN 5
            WHEN 'junio'  THEN 6
            WHEN 'julio'  THEN 7
            WHEN 'sept.'  THEN 9
            WHEN 'oct.'   THEN 10
            WHEN 'nov.'   THEN 11
            WHEN 'dic.'   THEN 12
            ELSE 6
        END AS month_num
    FROM {{ source('bigdatamarina', 'elections_parlamento_vasco') }}
),

latest_month_per_year AS (
    SELECT year, MAX(month_num) AS max_month_num
    FROM with_month_num
    GROUP BY year
)

SELECT
    m.year,
    m.party_name,
    m.seats AS total_seats
FROM with_month_num m
JOIN latest_month_per_year l
  ON m.year = l.year AND m.month_num = l.max_month_num
ORDER BY m.year, m.seats DESC
