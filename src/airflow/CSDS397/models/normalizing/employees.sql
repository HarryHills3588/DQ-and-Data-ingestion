select
    ec.employee_id,
    ec.name,
    ec.age,
    d.department_id,
    ec.date_of_joining,
    ec.years_of_experience,
    c.country_id,
    ec.salary,
    ec.performance_rating
from {{ source('sources', 'employees_clean') }} ec
join {{ ref('departments') }} d on d.department_name = ec.department
join {{ ref('countries') }} c on c.country_name = ec.country