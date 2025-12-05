-- We want to know retention week by week

with weeks as (
    select 
        date_trunc('week', period_start::timestamp) as week_start,
        user_id
    from {{ ref('stg_enabled_listing_periods') }}
    group by 1, 2
),

retention as (
    select
        w.user_id,
        exp.experiment_group as group_name,
        w.week_start,
        1 as is_retained
    from weeks w
    left join {{ ref('stg_experiment') }} exp 
        on w.user_id = exp.user_id
)

select *
from retention
order by user_id, week_start
