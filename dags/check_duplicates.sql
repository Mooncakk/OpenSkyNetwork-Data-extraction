select
    callsign, timestamp, count(*) as cnt
from {{ ti.xcom_pull(task_ids='run_parameters', key ='target_table') }}
group by 1, 2
having cnt > 1;