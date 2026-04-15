{{ config(tags=['daily']) }}

with sales as (
    select * from {{ ref('stg_sales_orders') }}
),

contractors as (
    select * from {{ ref('stg_contractors') }}
)

select c.contractor_id, c.contractor_name, c.country, c.client_type,
    count(s.order_id) as total_orders, sum(s.quantity) as total_units,
    sum(s.total_price) as total_revenue, avg(s.total_price) as avg_order_value,
    dense_rank() over (order by sum(s.total_price) desc) as revenue_rank
from sales s
left join contractors c on s.contractor_id = c.contractor_id
where s.status != 'terminated'
group by c.contractor_id, c.contractor_name, c.country, c.client_type