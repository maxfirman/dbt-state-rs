{{ config(materialized='view') }}
select id, amount from {{ ref('orders') }}
