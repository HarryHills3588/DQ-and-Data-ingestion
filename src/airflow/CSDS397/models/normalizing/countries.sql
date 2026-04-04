WITH distinct_countries AS (
    SELECT DISTINCT
        country AS country_name
    FROM {{ source('sources', 'employees_clean') }}
    WHERE country IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY country_name) AS country_id,
    country_name
FROM distinct_countries