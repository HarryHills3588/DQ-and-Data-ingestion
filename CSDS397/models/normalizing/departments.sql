select distinct
    row_number() over (order by department) as department_id,
    department as department_name
from {{ source('sources', 'employees_clean') }}
where department is not null