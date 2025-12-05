{{ config(materialized="view") }}

select *
from {{ ref('statement_items') }}