with source as (
    select * from {{ ref('raw_employee_attendance') }}
)

select employee_id, cast(date as date) as attendance_date,
    status, check_in, check_out, hours_worked
from source