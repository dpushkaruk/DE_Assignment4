with source as (
    select * from {{ ref('raw_negotiation_rounds') }}
)

select negotiation_id, round_number,
    cast(date as date) as round_date, proposed_by,
    unit_price_proposed, total_proposed, outcome, notes
from source