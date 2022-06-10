from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def virtualenv_fn():
    import boto3
    print("boto3 version ",boto3.__version__)

with DAG(dag_id="virtualenv_test", schedule_interval=None, catchup=False, start_date=days_ago(1)) as dag:
    virtualenv_task = PythonVirtualenvOperator(
        task_id="virtualenv_task",
        python_callable=virtualenv_fn,
        requirements=["boto3>=1.17.43"],
        system_site_packages=False,
        dag=dag,
    )
    # make new venv => /usr/local/bin/python -m virtualenv /tmp/venv_ly1_wnn
#     t1 = BashOperator(
#         task_id='print_date',
#         bash_command='date',
#     )
    
#     t1