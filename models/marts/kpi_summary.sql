-- Activation rate (A vs B), Monetization rate (A vs B), Revenue differences and Final results

with act as (
    select * from {{ ref('activation') }}
),
mon as (
    select * from {{ ref('monetization') }}
),
ret as (
    select * from {{ ref('weekly_retention') }}
)

select
    exp.experiment_group as group_name,
    avg(act.is_activated) as activation_rate,
    avg(case when mon.revenue > 0 then 1 else 0 end) as monetization_rate,
    avg(mon.revenue) as avg_revenue_per_user
from {{ ref('stg_experiment') }} exp
left join act
    on exp.user_id = act.user_id
left join mon
    on exp.user_id = mon.user_id
group by 1
