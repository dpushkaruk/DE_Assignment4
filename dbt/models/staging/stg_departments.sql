with source as (
    select * from {{ ref('raw_departments') }}
)

select department_id, name as department_name, head as head_employee_id
from source