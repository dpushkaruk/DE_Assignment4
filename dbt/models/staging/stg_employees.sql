with source as (
    select * from {{ ref('raw_employees') }}
)

select employee_id, first_name, last_name, 
    first_name || ' ' || last_name as full_name,
    email, role, salary, department_id,
    cast(hire_date as date) as hire_date,
    cast(is_active as boolean) as is_active
from source