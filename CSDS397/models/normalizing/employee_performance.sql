select
    employee_id,
    total_sales,
    support_rating
from {{ source('sources', 'employees_clean') }}
where total_sales > 0 or support_rating > 0