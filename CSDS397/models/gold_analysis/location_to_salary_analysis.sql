SELECT
   c.country_name,
   ROUND(AVG(e.salary), 2) AS avg_salary,
   ROUND(AVG(e.performance_rating), 2) AS avg_performance_rating,
   ROUND(AVG(ep.total_sales), 2) AS avg_total_sales
FROM {{ ref('employees') }} e JOIN {{ ref('countries' )}} c ON e.country_id = c.country_id
LEFT JOIN {{ ref('employee_performance') }} ep ON e.employee_id = ep.employee_id
GROUP BY
   c.country_id,
   c.country_name
ORDER BY avg_salary DESC