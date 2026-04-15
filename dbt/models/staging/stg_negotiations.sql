with source as (
    select * from {{ ref('raw_negotiations') }}
)

select negotiation_id, cast(linked_order_id as bigint) as linked_order_id,
    contractor, product_code, quantity,initial_list_price,
    cast(start_date as date) as start_date,
    cast(last_update as date) as last_update,
    num_rounds, final_outcome, final_unit_price, final_total_value,
    currency, fire_point_contact, contractor_contact,
    case
        when final_unit_price is not null and initial_list_price > 0
        then round((1 - final_unit_price / initial_list_price) * 100, 2)
        else null
    end as discount_pct
from source