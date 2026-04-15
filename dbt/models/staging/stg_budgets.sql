with source as (
    select * from {{ ref('raw_budgets') }}
)

select budget_id, department_id, amount as budget_amount,
    spent as budget_spent, amount - spent as budget_remaining,
    fiscal_year, round(spent * 100.0 / amount, 2) as utilization_pct
from source