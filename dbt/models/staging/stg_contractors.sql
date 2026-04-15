with source as (
    select * from {{ ref('raw_contractors') }}
)

select contractor_id, name as contractor_name, client_type,
    contact_name, contact_email, phone,
    upper(trim(country)) as country
from source