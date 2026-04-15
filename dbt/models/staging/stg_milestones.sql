with source as (
    select * from {{ ref('raw_milestones') }}
)

select milestone_id, project_id,
    name as milestone_name, description,
    cast(due_date as date) as due_date,
    cast(completed_date as date) as completed_date,
    status, case when completed_date is not null and due_date is not null
        then completed_date - due_date
        else null end as days_overdue
from source