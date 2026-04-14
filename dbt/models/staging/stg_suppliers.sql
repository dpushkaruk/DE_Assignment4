with source as (
    select * from {{ ref('raw_suppliers') }}
)

select supplier_id, name as supplier_name, contact_name,
    contact_email,phone, upper(trim(country)) as country,
    trim(city) as city
from source