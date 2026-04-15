{{ config(tags=['daily']) }}

with purchases as (
    select * from {{ ref('stg_purchase_orders') }}
),

components as (
    select * from {{ ref('stg_components') }}
),

suppliers as (
    select * from {{ ref('stg_suppliers') }}
)

select s.supplier_id, s.supplier_name, s.country,
    count(po.order_id) as total_orders, sum(po.quantity) as total_units_ordered,
    avg(po.lead_time_days) as avg_lead_time_days,
    sum(case when po.status = 'delivered' then 1 else 0 end) as delivered_count,
    round(sum(case when po.status = 'delivered' then 1 else 0 end) * 100.0 / count(*), 2) as delivery_rate_pct,
    rank() over (order by count(po.order_id) desc) as rank_by_order_volume
from purchases po
left join components c on po.component_id = c.component_id
left join suppliers s on c.supplier_id = s.supplier_id
group by s.supplier_id, s.supplier_name, s.country