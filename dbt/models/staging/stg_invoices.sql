with source as (
    select * from {{ ref('raw_invoices') }}
)

select invoice_id, source_type, source_id,
    cast(client_id as bigint) as client_id,
    cast(supplier_id as bigint) as supplier_id,
    amount, currency,
    cast(issue_date as date) as issue_date,
    cast(due_date as date) as due_date,
    status
from source