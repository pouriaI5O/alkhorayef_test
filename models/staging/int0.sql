{{ config(materialized='ephemeral') }}
with cte as (select start_time,
end_time,
camera,
status as  old_status,
time_zone, case 
when (status ='idle-time' or status='background')then 'idle'
 else status end as new_status

 from  {{ ref('src_test')}})

select start_time,
end_time,
camera,
time_zone,
new_status as status from cte