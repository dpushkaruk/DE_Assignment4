{{ config(tags=['daily']) }}

with production as (
    select * from {{ ref('stg_production_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
)

select po.production_line_id, po.product_id, p.product_name,
    po.status, count(*) as total_orders, sum(po.quantity) as total_units,
    avg(po.production_days) as avg_production_days,
    avg(avg(po.production_days)) over (partition by po.production_line_id order by po.product_id rows between 2 preceding and current row) as rolling_avg_production_days
from production po
left join products p on po.product_id = p.product_id
group by po.production_line_id, po.product_id, p.product_name, po.status