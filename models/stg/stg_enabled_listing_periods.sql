{{ config(materialized="view") }}

select 
    user_id,
    listing_id,
    period_start::timestamp as period_start,
    period_end::timestamp as period_end
from {{ ref('enabled_listing_periods') }}