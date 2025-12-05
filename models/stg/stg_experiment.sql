{{ config(materialized="view") }}

select 
    user_id,
    signup_date::timestamp as signup_date,
    country,
    experiment_group
from {{ ref('experiment') }}
