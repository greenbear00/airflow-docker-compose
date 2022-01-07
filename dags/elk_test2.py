import os
import sys
from datetime import datetime, timedelta
import pprint

sys.path.append('/opt/airflow/repository')
for s in sys.path:
    print(s)


from main.job.MappingJTBCTVProRunner import runner
from airflow import DAG
from airflow.decorators import task

target_date = datetime.now()
with DAG(
    dag_id='example_python_operator',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_python]
    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    run_this = print_context()

