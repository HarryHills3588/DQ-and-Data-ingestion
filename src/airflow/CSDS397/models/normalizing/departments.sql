WITH distinct_departments AS (
    SELECT DISTINCT
        department AS department_name
    FROM {{ source('sources', 'employees_clean') }}
    WHERE department IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY department_name) AS department_id,
    department_name
FROM distinct_departments