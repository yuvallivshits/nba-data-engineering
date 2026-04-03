from datetime import datetime
from airflow.sdk import DAG
from airflow.operators.python import PythonOperator


def hello():
    print("Hello from Airflow 3!")


with DAG(
    dag_id="example_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    task = PythonOperator(
        task_id="hello",
        python_callable=hello,
    )
