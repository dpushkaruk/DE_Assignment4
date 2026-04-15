{{ config(tags=['daily']) }}

with negotiations as (
    select * from {{ ref('stg_negotiations') }}
)

select contractor, product_code, count(*) as total_negotiations,
    sum(case when final_outcome = 'contract_signed' then 1 else 0 end) as won_deals,
    round(sum(case when final_outcome = 'contract_signed' then 1 else 0 end) * 100.0 / count(*), 2 ) as win_rate_pct,
    avg(discount_pct) as avg_discount_pct, sum(final_total_value) as total_contract_value, 
    avg(num_rounds) as avg_negotiation_rounds,
    row_number() over (partition by product_code order by sum(final_total_value) desc nulls last) as contractor_rank_by_value
from negotiations
group by contractor, product_code