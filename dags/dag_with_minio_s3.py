from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # type: ignore

default_args = {
    'owner': 'nttung',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}



with DAG(
    default_args = default_args,
    dag_id = 'dag_with_minio_s3_v02',
    start_date =datetime(2025,4,12),
    schedule_interval = '@daily'
) as dag:
    task1 = S3KeySensor(
        task_id = 'sensor_minio_s3',
        bucket_name = 'etl-bucket',
        bucket_key = 'Hello.txt',
        aws_conn_id = 'minio_conn',
        mode='poke',
        poke_interval=5,
        timeout =30
    )
