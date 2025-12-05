-- We want to know which users activated, activation date and whether activation differs by group A vs B

with enabled as (
    select 
        user_id,
        min(period_start) as first_enabled_date
    from {{ ref('stg_enabled_listing_periods') }}
    group by 1
)

select
    e.user_id,
    exp.experiment_group as group_name,
    e.first_enabled_date,
    case when e.first_enabled_date is not null then 1 else 0 end as is_activated
from enabled e
left join {{ ref('stg_experiment') }} exp 
    on e.user_id = exp.user_id
