{{ config(tags=['daily']) }}

with attendance as (
    select * from {{ source('firepoint', 'employee_attendance') }}
),

employees as (
    select * from {{ ref('stg_employees') }}
)

select e.department_id, date_trunc('month', cast(a.date as date)) as attendance_month,
    count(distinct a.employee_id) as active_employees, sum(case when a.status = 'present' then 1 else 0 end) as present_days,
    sum(case when a.status = 'sick_leave' then 1 else 0 end) as sick_days,
    round(sum(case when a.status = 'present' then 1 else 0 end) * 100.0 / count(*), 2) as attendance_rate_pct,
    avg(a.hours_worked) as avg_hours_worked,
    lag(round(sum(case when a.status = 'present' then 1 else 0 end) * 100.0 / count(*), 2)) over (
        partition by e.department_id order by date_trunc('month', cast(a.date as date))
    ) as prev_month_attendance_rate
from attendance a
left join employees e on a.employee_id = e.employee_id
group by e.department_id, date_trunc('month', cast(a.date as date))