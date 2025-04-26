from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore

default_args = {
    'owner': 'nttung',
    'reties':5,
    'retry_delay': timedelta(minutes = 5)
}

with DAG(
    dag_id = 'dag_with_catchup_backfill_v02',
    default_args = default_args,
    start_date = datetime(2025,4,1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    task1  = BashOperator(
        task_id = 'task1',
        bash_command = 'echo This is a simple bash command!'
    )