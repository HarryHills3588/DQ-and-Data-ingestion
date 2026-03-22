CREATE TABLE gold.department_to_performance_salary AS
SELECT
   d.department_name,
   e.performance_rating,
   ROUND(AVG(e.salary), 2) AS avg_salary,
   ROUND(AVG(ep.total_sales), 2) AS avg_total_sales
FROM staging.employees e
JOIN staging.departments d ON e.department_id = d.department_id
LEFT JOIN staging.employee_performance ep ON e.employee_id = ep.employee_id
GROUP BY
   d.department_name,
   e.performance_rating
ORDER BY
   d.department_name,
   e.performance_rating;
