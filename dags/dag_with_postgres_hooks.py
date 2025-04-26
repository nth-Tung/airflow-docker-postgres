import csv
import logging
from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # type: ignore
from tempfile import NamedTemporaryFile

default_args = {
    'owner':'nttung',
    'retries':5,
    'retry_delay':timedelta(minutes=10)
}


def postgres_to_s3(ds_nodash,next_ds_nodash):
    hook = PostgresHook(postgres_conn_id ='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders")
    with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}") as f:
    # with open(f"dags/get_orders{ds_nodash}.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
        logging.info("Saved orders data in text file: %s", f"dags/get_orders{ds_nodash}.txt")
        s3_hook = S3Hook(aws_conn_id ='minio_conn')
        s3_hook.load_file(
            filename = f.name,
            key = f"orders/{ds_nodash}.txt",
            bucket_name = "etl-bucket",
            replace = True
        )
        logging.info("Orders file %s has been pushed to S3!", f.name)

with DAG(
    default_args = default_args,
    dag_id = 'dag_with_postgres_hooks_v05',
    start_date = datetime(2022,3,12),
    schedule_interval = '@daily'
) as dag:
    task1 = PythonOperator(
        task_id = "postgres_to_s3",
        python_callable = postgres_to_s3
    )

    task1