{{ config(tags=['daily']) }}

with sales as (
    select * from {{ ref('stg_sales_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
)

select s.product_id, p.product_name, p.product_code,
    date_trunc('month', s.order_date) as order_month,
    count(*) as total_orders, sum(s.quantity) as total_units,
    sum(s.total_price) as total_revenue, avg(s.total_price) as avg_order_value,
    sum(sum(s.total_price)) over ( partition by s.product_id order by date_trunc('month', s.order_date) ) as cumulative_revenue
from sales s
left join products p on s.product_id = p.product_id
where s.status != 'cancelled'
group by s.product_id, p.product_name, p.product_code, date_trunc('month', s.order_date)