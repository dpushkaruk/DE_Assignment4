{{ config(tags=['daily']) }}

with payments as (
    select * from {{ ref('stg_payments') }}
),

invoices as (
    select * from {{ ref('stg_invoices') }}
)

select i.source_type, date_trunc('month', p.payment_date) as payment_month,
    p.method as payment_method, count(*) as total_payments, sum(p.amount) as total_amount, avg(p.amount) as avg_payment_amount,
    sum(sum(p.amount)) over (partition by i.source_type order by date_trunc('month', p.payment_date)) as cumulative_amount
from payments p
left join invoices i on p.invoice_id = i.invoice_id
group by i.source_type, date_trunc('month', p.payment_date), p.method