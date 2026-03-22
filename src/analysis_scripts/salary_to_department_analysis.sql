CREATE TABLE gold.salary_to_department_analysis AS
SELECT d.department_id, d.department_name, ROUND(AVG(e.salary), 2) AS avg_salary
FROM staging.employees e JOIN staging.departments d ON e.department_id = d.department_id
GROUP BY d.department_id, d.department_name
ORDER BY avg_salary DESC;