with source as (
    select * from {{ ref('raw_products') }}
)

select product_id, name as product_name, product_code, product_class,
    description, max_range_km, max_speed_ms, base_unit_price, unit_cost,
    base_unit_price - unit_cost as unit_margin,
    cast(is_active as boolean) as is_active, created_at
from source