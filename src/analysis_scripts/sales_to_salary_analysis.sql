CREATE TABLE gold.sales_to_salary_analysis AS SELECT
   CASE
       WHEN ep.total_sales < 50000 THEN 'Under $50K'
       WHEN ep.total_sales BETWEEN 50000  AND 99999 THEN '$50K - $99K'
       WHEN ep.total_sales BETWEEN 100000 AND 199999 THEN '$100K - $199K'
       WHEN ep.total_sales BETWEEN 200000 AND 299999 THEN '$200K - $299K'
       ELSE '$300K+'
   END AS sales_bucket,
   ROUND(AVG(e.salary), 2) AS avg_salary,
   ROUND(AVG(e.performance_rating), 2) AS avg_performance_rating,
   ROUND(AVG(ep.total_sales), 2) AS avg_total_sales
FROM staging.employees e
JOIN staging.employee_performance ep ON e.employee_id = ep.employee_id
GROUP BY
   sales_bucket
ORDER BY avg_total_sales ASC 