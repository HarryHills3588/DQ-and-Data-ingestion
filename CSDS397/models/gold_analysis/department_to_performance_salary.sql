select
   d.department_name,
   e.performance_rating,
   round(avg(e.salary), 2) as avg_salary,
   round(avg(ep.total_sales), 2) as avg_total_sales
from {{ ref('employees') }} e
join {{ ref('departments') }} d on e.department_id = d.department_id
left join {{ ref('employee_performance') }} ep on e.employee_id = ep.employee_id
group by
   d.department_name,
   e.performance_rating
order by
   d.department_name,
   e.performance_rating