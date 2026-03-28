select distinct
    row_number() over (order by country) as country_id,
    country as country_name
from {{ source('sources', 'employees_clean') }}
where country is not null