from airflow import DAG # type: ignore
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator # type: ignore


default_args = {
    'owner' : 'nttung',
    'reties' :5,
    'retry_delay': timedelta(minutes= 5)
}

def greet(ti):
    firt_name = ti.xcom_pull(task_ids='get_name', key = 'first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key = 'last_name')
    age = ti.xcom_pull(task_ids ='get_age', key ='age')
    print(f"Hello world! My name is {firt_name} {last_name},"
          f"and I am {age} years old!")

def get_name(ti):
    ti.xcom_push(key='first_name', value = 'Tung')
    ti.xcom_push(key='last_name', value = 'Nguyen')

def get_age(ti):
    ti.xcom_push(key= 'age', value = 21)

with DAG(
    default_args = default_args,
    dag_id = 'our_dag_with_python_operator_v06',
    description = 'Our first dag using python operator',
    start_date = datetime(2025, 4, 11),
    schedule_interval = '@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable = greet
        # op_kwargs = {'age': 21}
    )
    
    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable = get_name
    )

    task3 = PythonOperator(
        task_id = 'get_age',
        python_callable = get_age
    )

    [task2, task3] >> task1