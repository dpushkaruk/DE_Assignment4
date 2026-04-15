with source as (
    select * from {{ ref('raw_production_stages') }}
)

select stage_id, prod_order_id, stage_name,
    cast(start_time as timestamp) as start_time,
    cast(end_time as timestamp) as end_time,
    responsible as responsible_employee_id,
    status, case when end_time is not null and start_time is not null
        then extract(epoch from (end_time - start_time)) / 3600.0
        else null end as duration_hours
from source