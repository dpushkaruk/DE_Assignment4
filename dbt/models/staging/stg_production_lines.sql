with source as (
    select * from {{ ref('raw_production_lines') }}
)

select line_id as production_line_id, name as line_name,
    facility, supervisor as supervisor_employee_id
from source