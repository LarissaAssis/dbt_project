{{ config(materialized="view") }}

select 
    user_id,
    created_at::timestamp as created_at,
    id,
    ROUND(amount_usd::numeric, 2) as amount_usd
from {{ ref('statement_items') }}