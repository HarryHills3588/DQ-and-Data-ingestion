SELECT
   e.performance_rating,
   ROUND(AVG(e.salary), 2) AS avg_salary
FROM {{ ref('employees' )}} e
LEFT JOIN {{ ref('employee_performance' )}} ep ON e.employee_id = ep.employee_id
GROUP BY
   e.performance_rating
ORDER BY e.performance_rating