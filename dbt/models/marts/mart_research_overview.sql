{{ config(tags=['daily']) }}

with projects as (
    select * from {{ ref('stg_research_projects') }}
),

milestones as (
    select * from {{ ref('stg_milestones') }}
),

products as (
    select * from {{ ref('stg_products') }}
)

select p.project_id, p.project_name, pr.product_name, p.status as project_status, p.budget, p.spent,
    p.budget_utilization_pct, p.start_date, p.expected_end_date,
    count(m.milestone_id) as total_milestones,
    sum(case when m.status = 'completed' then 1 else 0 end) as completed_milestones,
    round(sum(case when m.status = 'completed' then 1 else 0 end) * 100.0 / nullif(count(m.milestone_id), 0), 2) as milestone_completion_pct,
    avg(m.days_overdue) as avg_days_overdue
from projects p
left join milestones m on p.project_id = m.project_id
left join products pr on p.product_id = pr.product_id
group by p.project_id, p.project_name, pr.product_name, p.status, p.budget, p.spent,
         p.budget_utilization_pct, p.start_date, p.expected_end_date