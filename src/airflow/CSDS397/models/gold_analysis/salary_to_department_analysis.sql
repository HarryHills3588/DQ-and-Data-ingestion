SELECT d.department_id, d.department_name, ROUND(AVG(e.salary), 2) AS avg_salary
FROM {{ ref('employees' )}} e JOIN {{ ref('departments') }} d ON e.department_id = d.department_id
GROUP BY d.department_id, d.department_name
ORDER BY avg_salary DESC