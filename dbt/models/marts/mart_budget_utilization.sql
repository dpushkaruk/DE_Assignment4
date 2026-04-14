{{ config(tags=['daily']) }}

with budgets as (
    select * from {{ source('firepoint', 'budgets') }}
),

departments as (
    select * from {{ source('firepoint', 'departments') }}
)

select b.budget_id, b.department_id, d.name as department_name,
    b.fiscal_year, b.amount as budget_amount, b.spent as budget_spent,
    b.amount - b.spent as budget_remaining,
    round(b.spent * 100.0 / b.amount, 2) as utilization_pct,
    case
        when b.spent * 100.0 / b.amount > 90 then 'critical'
        when b.spent * 100.0 / b.amount > 75 then 'warning'
        else 'healthy'
    end as budget_status
from budgets b
left join departments d on b.department_id = d.department_id