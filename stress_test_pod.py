
from airflow import DAG
from airflow import configuration as conf
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': '<uremailid>',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'kubernetes_sample', default_args=default_args, catchup=False, schedule_interval=timedelta(minutes=10))

start = DummyOperator(task_id='run_this_first', dag=dag)

task1 = KubernetesPodOperator(namespace='default',
                                image="python:3.6",
                                cmds=["python", "-c"],
                                arguments=["print('hello world')"],
                                labels={"test-airflow": "firstversion"},
                                name="task1",
                                task_id="task1-task",
                                get_logs=True,
                                dag=dag
                                )

task2 = KubernetesPodOperator(namespace='default',
                                image="progrium/stress",
                                cmds=["bash","-cx"],
                                arguments=["stress --cpu 4 --io 2- -vm 1--vm-bytes 128M --timeout 5s"],
                                labels={"stress": "test"},
                                name="task2",
                                task_id="task2-task",
                                get_logs=True,
                                dag=dag
                                )

task1.set_upstream(start)
task2.set_upstream(start)