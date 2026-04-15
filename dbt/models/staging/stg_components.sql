with source as (
    select * from {{ ref('raw_components') }}
)

select component_id, name as component_name,
    category, description, price, supplier_id
from source