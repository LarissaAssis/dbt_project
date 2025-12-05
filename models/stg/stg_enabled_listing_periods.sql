{{ config(materialized="view") }}

select *
from {{ ref('enabled_listing_periods') }}