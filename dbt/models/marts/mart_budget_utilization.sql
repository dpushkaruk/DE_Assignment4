{{ config(tags=['daily']) }}

with budgets as (
    select * from {{ ref('stg_budgets') }}
),

departments as (
    select * from {{ ref('stg_departments') }}
)

select b.budget_id, b.department_id, d.department_name,
    b.fiscal_year, b.budget_amount, b.budget_spent,
    b.budget_remaining, b.utilization_pct, 
    case
        when b.utilization_pct > 90 then 'critical'
        when b.utilization_pct > 75 then 'warning'
        else 'healthy'
    end as budget_status
from budgets b
left join departments d on b.department_id = d.department_id