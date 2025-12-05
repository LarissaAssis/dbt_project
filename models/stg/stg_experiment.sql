{{ config(materialized="view") }}

select *
from {{ ref('experiment') }}