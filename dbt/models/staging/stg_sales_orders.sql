with source as (
    select * from {{ ref('raw_sales_orders') }}
)

select order_id, contractor_id, product_id, quantity,
    total_price, currency, cast(order_date as date) as order_date,
    cast(delivery_deadline as date) as delivery_deadline,
    cast(actual_delivery_date as date) as actual_delivery_date, status,
    responsible as responsible_employee_id,
    case
        when actual_delivery_date is not null and actual_delivery_date <= delivery_deadline then true
        when actual_delivery_date is not null and actual_delivery_date > delivery_deadline then false
        else null
    end as is_on_time
from source