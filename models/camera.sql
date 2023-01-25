{{ config(
    materialized='table'
)}}
select * from {{ ref('int1')}}