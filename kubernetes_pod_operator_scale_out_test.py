from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow() - timedelta(days=1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'eks_scale_out_test', default_args=default_args, schedule_interval=timedelta(minutes=10), catchup=False)

start = DummyOperator(task_id='run_this_first', dag=dag)

task1 = KubernetesPodOperator(namespace='default',
                          image="python:3.6",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="task1-test",
                          task_id="task1-task",
                          get_logs=True,
                          dag=dag
                          )

task2 = KubernetesPodOperator(namespace='default',
                          image="ubuntu:1604",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="task2-test",
                          task_id="task2-task",
                          get_logs=True,
                          dag=dag
                          )

task3 = KubernetesPodOperator(namespace='default',
                          image="ubuntu:1604",
                          cmds=["sudo","apt","-y","install","stress-ng","&&","stress-ng","--cpu","64","--cpu-method all","--verify","-t","5m","--metrics-brief"],
                          labels={"stress": "test"},
                          name="task3-test",
                          task_id="task3-task",
                          get_logs=True,
                          dag=dag
                          )

task1.set_upstream(start)
task2.set_upstream(start)
task3.set_upstream(start)
