with source as (
    select * from {{ ref('raw_purchase_orders') }}
)

select order_id, component_id, quantity,
    cast(order_date as date) as order_date,
    cast(delivery_date as date) as delivery_date,
    status, responsible as responsible_employee_id,
    case
        when delivery_date is not null then delivery_date - order_date
        else null
    end as lead_time_days
from source