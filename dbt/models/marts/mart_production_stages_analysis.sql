{{ config(tags=['daily']) }}

with stages as (
    select * from {{ ref('stg_production_stages') }}
),

orders as (
    select * from {{ ref('stg_production_orders') }}
),

lines as (
    select * from {{ ref('stg_production_lines') }}
)

select l.production_line_id, l.line_name, l.facility, s.stage_name,
    count(*) as total_stages, avg(s.duration_hours) as avg_duration_hours,
    sum(case when s.status = 'completed' then 1 else 0 end) as completed_count,
    round( sum(case when s.status = 'completed' then 1 else 0 end) * 100.0 / count(*), 2) as completion_rate_pct,
    avg(avg(s.duration_hours)) over (partition by l.production_line_id order by s.stage_name
        rows between 2 preceding and current row) as rolling_avg_duration
from stages s
left join orders o on s.prod_order_id = o.prod_order_id
left join lines l on o.production_line_id = l.production_line_id
group by l.production_line_id, l.line_name, l.facility, s.stage_name