{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='id', on_schema_change='fail') }}
select id, amount from warehouse.raw.inputs
