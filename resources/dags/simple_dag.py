import airflow

from datetime import date, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "provide_context": True,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "simple_dag",
    description="A simple DAG",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    tags=["example"],
)


def hello_world():
    print("Hello World!")

    return str(date.today())


task1 = PythonOperator(
    task_id="hello_world",
    python_callable=hello_world,
    provide_context=True,
    dag=dag
)


def today_is(name, **context):
    res = context["task_instance"].xcom_pull(task_ids="hello_world")
    print(f"{name}, today is {res}")


task2 = PythonOperator(
    task_id="today_is",
    python_callable=today_is,
    provide_context=True,
    op_kwargs={"name": "AWS"},
    dag=dag
)


def good_day():
    print("Have a good day!")


task3 = PythonOperator(
    task_id="good_day",
    python_callable=good_day,
    provide_context=True,
    dag=dag
)


task1 >> task2 >> task3
