with source as (
    select * from {{ ref('raw_production_orders') }}
)

select prod_order_id, product_id, production_line_id, quantity,
    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date, status,
    responsible as responsible_employee_id,
    case
        when end_date is not null then end_date - start_date
        else null
    end as production_days
from source