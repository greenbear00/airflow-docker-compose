from datetime import datetime
from textwrap import dedent

from airflow.models import DAG
from airflow.operators.bash import BashOperator

DEFAULT_DATE = datetime(2016, 1, 1)

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE,
}

run_as_user = 'default'

with DAG(dag_id='test_impersonation', default_args=args) as dag:

    test_command = """
    hostname
    
    """

    task1 = BashOperator(
        task_id='test_impersonated_user',
        bash_command=test_command,
        dag=dag,
        run_as_user=run_as_user,
    )
    task2 = BashOperator(
        task_id='test_impersonated_user2',
        bash_command="whoami",
        dag=dag,
        run_as_user=run_as_user,
    )
    task3 = BashOperator(
        task_id='test_impersonated_user3',
        bash_command="sudo ls -l",
        dag=dag,
        run_as_user=run_as_user,
    )
    
    task1 >> task2 >> task3
