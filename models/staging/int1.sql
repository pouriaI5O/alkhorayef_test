{{ config(materialized='ephemeral') }}

with cte as (select *,
        EXTRACT(HOUR FROM end_time) AS Hour,
        EXTRACT(minute FROM end_time) AS Minute,
        EXTRACT(second from end_time) as second,  
        DATE(end_time) as Date
from {{ ref('int0')}}),

cte1 as (select * from cte where date in ('2023-01-04','2023-01-05','2023-01-07')),
cte2 as (select camera,status,date,hour,minute,second from cte1 where camera in ( 'w4c7', 'w4c8'))

select * from cte2
         