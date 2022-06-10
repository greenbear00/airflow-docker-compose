# 설치가 필요한 패키지가 모든 노드에 제대로 설치 되었는지 체크하기
# 각 노드(worker)별로 아래와 같은 명령어로 설치 필요함
# sudo docker exec -it --user airflow airflow_flower.1.rns44d5aikcgyojuzoo4izgd3 bash
# python -m pip install --user rootpath 

import os
import sys
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/repository/jnd-batch')
for s in sys.path:
    print(s)


from greenbear.job.MappingRunner import rootpath_detect
from airflow.operators.python import PythonOperator
from airflow import DAG


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'rootpath_detect',
    default_args=default_args,
    description='rootpath_detect test',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 12, 14),
    catchup=False,
    tags=['example'],
) as dag:
    # [END instantiate_dag]

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # [START basic_task]
    t1 = PythonOperator(
        task_id='MappingRunner',
        provide_context=True,
        python_callable=rootpath_detect,
        op_kwargs={
            'target_date': datetime.now()
        }
    )

    t1

