with source as (
    select * from {{ ref('raw_payments') }}
)

select payment_id,  invoice_id, amount,
    cast(payment_date as date) as payment_date, method
from source