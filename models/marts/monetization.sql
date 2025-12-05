-- We want to know how much revenue each user generated and whether group A (credit) monetizes differently than group B (no credit)

select
    s.user_id,
    exp.experiment_group as group_name,
    sum(s.amount_usd) as revenue,
    count(*) as bookings_count
from {{ ref('stg_statement_items') }} s
left join {{ ref('stg_experiment') }} exp
    on s.user_id = exp.user_id
group by 1, 2
