from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore


default_args = {
    'owner': 'nttung',
    'retries': 5,
    'retry_delay': timedelta(minutes = 5)
}


with DAG(
    dag_id = 'dag_with_cron_expression_v05',
    default_args = default_args,
    start_date = datetime(2025,3,25,2),
    schedule_interval = '0 3 * * Tue-Sat'
) as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command = 'echo dag with cron expression'
    )

    task1