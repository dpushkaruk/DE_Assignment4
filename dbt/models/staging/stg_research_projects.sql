with source as (
    select * from {{ ref('raw_research_projects') }}
)

select project_id, name as project_name,
    cast(product_id as bigint) as product_id,
    lead_researcher as lead_researcher_employee_id,
    budget, spent, round(spent * 100.0 / nullif(budget, 0), 2) as budget_utilization_pct,
    cast(start_date as date) as start_date, status,
    cast(expected_end as date) as expected_end_date
from source