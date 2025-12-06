{{ config(materialized='view') }}

-- ============================================================
-- IMPORTANT INFORMATIONS

-- Base: stg_experiment (all experiment users)
-- Adds:
--   - activation (is_activated, activation_date)
--   - monetization (gross revenue, bookings, revenue_after_credit, credit_consumed, credit_remaining)
--   - weekly retention (total_weeks_active, is_retained_weekly)
-- All monetary calculations only count statement_items that occurred while a listing was enabled (within LAMEN periods)
-- For users in group A, a $30 signup credit is consumed first; revenue is only recognized after the credit is depleted
-- ============================================================


/* ---------- 1) Activation (earliest enabled period per user) ---------- */
with enabled as (
    select
        user_id,
        min(period_start) as first_enabled_date
    from {{ ref('stg_enabled_listing_periods') }}
    group by user_id
),

/* ---------- 2) Statements that occurred DURING enabled periods ---------- */
statements_in_enabled as (
    select
        s.user_id,
        s.created_at,
        s.id as statement_id,
        s.amount_usd::numeric as amount_usd,
        elp.period_start,
        elp.period_end
    from {{ ref('stg_statement_items') }} s
    -- Joining 'enabled_listing_periods' to 'statement_items' to ensure only counting bookings that happened while a listing was enabled for that user
    join {{ ref('stg_enabled_listing_periods') }} elp
      on s.user_id = elp.user_id
     and s.created_at between elp.period_start and elp.period_end
),

/* ---------- 3) Monetization per booking with credit consumption logic ---------- */
/*
  For each booking (statement) per user, let's compute:
    - cumulative sum up to and including that booking (cum_after)
    - cumulative sum before that booking (cum_before = cum_after - amount_usd)
    - recognized_from_booking = greatest(0, cum_after - credit) - greatest(0, cum_before - credit)
  This yields the portion of the booking that is recognized as revenue after the $30 credit is consumed.
  For control users (no credit), credit = 0 and recognized_from_booking = amount_usd.
*/

bookings_with_recognition as (
    select
        sie.user_id,
        sie.created_at,
        sie.statement_id,
        sie.amount_usd,
        sum(sie.amount_usd) over (
            partition by sie.user_id
            order by sie.created_at, sie.statement_id
            rows between unbounded preceding and current row
        ) as cum_after
    from statements_in_enabled sie
),

bookings_recognized as (
    select
        b.user_id,
        b.created_at,
        b.statement_id,
        b.amount_usd,
        b.cum_after,
        -- cum_before = cum_after - amount_usd
        (b.cum_after - b.amount_usd) as cum_before,
        -- Determining credit amount per user (30 for group A, 0 for group B)
        case when exp.experiment_group = 'A' then 30.0 else 0.0 end as user_credit
    from bookings_with_recognition b
    left join {{ ref('stg_experiment') }} exp
      on b.user_id = exp.user_id
),

-- Computing recognized amount per booking using the formula: recognized = greatest(0, cum_after - credit) - greatest(0, cum_before - credit)
bookings_recognized_final as (
    select
        br.user_id,
        br.created_at,
        br.statement_id,
        br.amount_usd,
        br.cum_before,
        br.cum_after,
        br.user_credit,
        ( greatest(0.0, br.cum_after - br.user_credit)
            - greatest(0.0, br.cum_before - br.user_credit)
        )::numeric as recognized_amount
    from bookings_recognized br
),

-- Aggregating monetization per user
monetization as (
    select
        brf.user_id,
        sum(brf.amount_usd) as total_revenue_gross,
        count(*) as bookings_count,
        sum(brf.recognized_amount) as revenue_after_credit,
        -- credit_consumed is the amount of credit used: for A it's min(credit, gross), for B it's 0
        case
            when exp.experiment_group = 'A' then least(30.0, sum(brf.amount_usd))
            else 0.0
        end as credit_consumed
    from bookings_recognized_final brf
    left join {{ ref('stg_experiment') }} exp on brf.user_id = exp.user_id
    group by brf.user_id, exp.experiment_group
),

/* ---------- 4) Weekly retention: active at the END of the week ---------- */
/*
   For every enabled period, generate the week_start values between period_start and period_end by 1 week.
   For each generated week_start, define week_end = week_start + interval '6 days' (end of that week).
   A listing is considered active at week's end if period_end >= week_end.
   Then count distinct weeks per user for which the user had at least one listing active at that week's end.
*/
user_weeks_active as (
    select distinct
        elp.user_id,
        gs.week_start::date as week_start
    from {{ ref('stg_enabled_listing_periods') }} elp
    cross join lateral
      generate_series(
        date_trunc('week', elp.period_start)::date,
        date_trunc('week', elp.period_end)::date,
        interval '1 week'
      ) as gs(week_start)
    -- Keeping only weeks where the listing was still active at the end of the week
    where elp.period_end >= (gs.week_start::timestamp + interval '6 days')
),

weekly_retention as (
    select
        user_id,
        count(distinct week_start) as total_weeks_active,
        case when count(distinct week_start) > 1 then 1 else 0 end as is_retained_weekly
    from user_weeks_active
    group by user_id
)

-- FINAL SELECT: one row per user in stg_experiment
select
    exp.user_id,
    exp.signup_date,
    exp.country,
    exp.experiment_group,

    -- activation
    case when e.first_enabled_date is not null then 1 else 0 end as is_activated,
    e.first_enabled_date as activation_date,

    -- monetization (coalesce to 0 for users without bookings)
    coalesce(m.total_revenue_gross, 0.0) as total_revenue_gross,
    coalesce(m.bookings_count, 0) as bookings_count,
    coalesce(m.revenue_after_credit, 0.0) as revenue_after_credit,
    coalesce(m.credit_consumed, 0.0) as credit_consumed,
    -- credit remaining (only meaningful for group A)
    case when exp.experiment_group = 'A' then greatest(0.0, 30.0 - coalesce(m.credit_consumed, 0.0)) else 0.0 end as credit_remaining,

    -- weekly retention
    coalesce(wr.total_weeks_active, 0) as total_weeks_active,
    coalesce(wr.is_retained_weekly, 0) as is_retained_weekly

from {{ ref('stg_experiment') }} exp
left join enabled e        on exp.user_id = e.user_id
left join monetization m   on exp.user_id = m.user_id
left join weekly_retention wr on exp.user_id = wr.user_id
