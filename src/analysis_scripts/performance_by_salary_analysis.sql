SELECT
   e.performance_rating,
   ROUND(AVG(e.salary), 2) AS avg_salary
FROM staging.employees e
LEFT JOIN staging.employee_performance ep ON e.employee_id = ep.employee_id
GROUP BY
   e.performance_rating
ORDER BY e.performance_rating;