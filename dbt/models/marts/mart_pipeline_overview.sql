{{ config(tags=['daily']) }}

with sales as (
    select * from {{ ref('stg_sales_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
)

select s.product_id, p.product_name, count(*) as total_orders,
    sum(case when s.status = 'draft' then 1 else 0 end) as draft_orders,
    sum(case when s.status = 'confirmed' then 1 else 0 end) as confirmed_orders,
    sum(case when s.status = 'in_production' then 1 else 0 end) as in_production_orders,
    sum(case when s.status = 'fulfilled' then 1 else 0 end) as fulfilled_orders,
    sum(case when s.status = 'cancelled' then 1 else 0 end) as cancelled_orders,
    sum(case when s.status = 'draft' then s.total_price else 0 end) as draft_value,
    sum(case when s.status = 'fulfilled' then s.total_price else 0 end) as fulfilled_value,
    round(sum(case when s.status = 'fulfilled' then 1 else 0 end) * 100.0 / count(*), 2) as fulfillment_rate_pct
from sales s
left join products p on s.product_id = p.product_id
group by s.product_id, p.product_name