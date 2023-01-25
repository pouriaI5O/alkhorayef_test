{{ config(materialized='ephemeral') }}

with cte as (select *,
        EXTRACT(HOUR FROM end_time) AS Hour,
        EXTRACT(minute FROM end_time) AS Minute,  
        DATE(end_time) as Date
from {{ ref('int0')}}),

cte1 as (select * from cte where date in ('2023-01-04','2023-01-05','2023-01-07')),
cte2 as (select camera,status,date,hour,minute from cte1 where camera in ('w2c3', 'w2c4', 'w3c5', 'w3c6', 'w4c7', 'w4c8')),
cte3 as(select camera,status,date,hour,minute,count(*) as total_second from cte2 group by camera,status,date,hour,minute)
select camera,status,date,hour,sum(total_second)/60 as total_minute from cte3 group by camera,status,date,hour
         