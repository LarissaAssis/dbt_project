#  Why all Monetization Metrics are ZERO?

After completing the full modeling required in the assignment, I noticed that all monetization metrics (bookings_count, total_revenue_gross, revenue_after_credit, credit_consumed) were zero, which initially confused me.

I wasn’t sure whether the dataset contained issues or if I had made a mistake, since this behavior is unusual. So I validated everything step by step.

## 1. Statement items must occur during an enabled listing period.

The assignment clearly states:

*"Revenue is only generated when a user has at least one enabled listing and receives a reservation while the listing is enabled (LAMEN period)."*

To validate this, I tested whether any ```statement_items``` matched ```enabled_listing_periods```:
```
select 
    s.user_id, 
    s.id, 
    s.created_at,
    e.period_start,
    e.period_end
from dbt_lassis.stg_statement_items s
join dbt_lassis.stg_enabled_listing_periods e
    on s.user_id = e.user_id
   and s.created_at between e.period_start and e.period_end;
```

This returned 137 statement items, meaning the raw data does contain bookings occurring during enabled listing periods.

## 2. However, none of these 137 bookings belong to users in the experiment groups.

I then checked whether any of those users appear in the experiment dataset:

```
select 
    count(distinct s.user_id)
from dbt_lassis.stg_statement_items s
join dbt_lassis.stg_enabled_listing_periods e
      on s.user_id = e.user_id
     and s.created_at between e.period_start and e.period_end
left join dbt_lassis.stg_experiment exp
      on s.user_id = exp.user_id
where exp.user_id is not null;
```
This returned 0, meaning that:

- Experiment users never appear in ```statement_items```

- Experiment users never received bookings, at least in this dataset

- Therefore, experiment users never generated revenue, gross or net

- No credit is consumed, because there are no actual bookings to apply it to

This aligns exactly with the assignment assumption:

*"Revenue only occurs after a booking is created while the listing is enabled."*

Since no experiment user has any booking during an enabled period, all monetization metrics must be zero.

## 3. The model is correct. The dataset simply contains no monetization events for experiment users.

Given the data constraints, the only possible correct output is:

```
bookings_count = 0        No experiment user has bookings during LAMEN periods.
total_revenue_gross = 0   No bookings → no revenue.
credit_consumed = 0       Credit is consumed only when bookings occur.
revenue_after_credit = 0  With no credit consumption and no bookings, revenue remains zero.
```

This is not a modeling error, as I initially thought. These results simply reflect the actual outcome of the experiment: none of the test users generated bookings during enabled listing periods.