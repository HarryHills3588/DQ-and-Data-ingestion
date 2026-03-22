CREATE TABLE gold.salary_to_tenure_analysis AS
SELECT
   CASE
       WHEN e.years_of_experience BETWEEN 0 AND 2 THEN '0-2 Years'
       WHEN e.years_of_experience BETWEEN 3 AND 5 THEN '3-5 Years'
       WHEN e.years_of_experience BETWEEN 6 AND 8 THEN '6-8 Years'
       WHEN e.years_of_experience BETWEEN 9 AND 11 THEN '9-11 Years'
       WHEN e.years_of_experience BETWEEN 12 AND 14 THEN '12-14 Years'
       ELSE '15+ Years'
   END AS experience_bucket,
   ROUND(AVG(e.salary), 2) AS avg_salary
FROM staging.employees e
GROUP BY avg_salary ASC