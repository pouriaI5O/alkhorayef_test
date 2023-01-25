----filtering break time--
{{ config(materialized='ephemeral') }}
with cte as (select *,
      cast(end_time AS time) as Time 
      from {{ source('public','alkhorayef_cams') }}) ,
cte1 as (select start_time,
                 end_time,
                 camera,
                 status,
                 time_zone, 
                 CASE 
                  WHEN Time>'11:30:00' and Time <'12:15:00'THEN 0
                  WHEN Time >'15:00:00' and Time <'15:15:00'THEN 0
                  WHEN Time >'00:00:00' and Time <'00:45:00'THEN 0
                  WHEN Time >'03:00:00' and Time <'03:15:00'THEN 0
                ELSE 1 END AS Break_Filter
from cte
where Break_Filter>0)
select start_time,
       end_time,
       camera,
       status,
       time_zone
from cte1 where end_time > '2023-01-03 23:59:59' and end_time<'2023-01-08 00:00:00' order by end_time asc