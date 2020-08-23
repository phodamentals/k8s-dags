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

dag = DAG('test_k8s_pod_operator', default_args=default_args, schedule_interval=timedelta(minutes=10), catchup=False)

start = DummyOperator(task_id='run_this_first', dag=dag)

# task1
task1 = KubernetesPodOperator(
    namespace='default',
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="task1-test",
    task_id="task1-task",
    get_logs=True,
    startup_timeout_seconds=600,
    dag=dag
    # resources={
    #     'request_cpu': '1000m',
    #     'request_memory': '2Gi',
    #     'limit_cpu': '1000m',
    #     'limit_memory': '2Gi'
    # }
)

# task2
task2 = KubernetesPodOperator(
    namespace='default',
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="task2-test",
    task_id="task2-task",
    get_logs=True,
    startup_timeout_seconds=600,
    dag=dag
    # resources={
    #     'request_cpu': '2000m',
    #     'request_memory': '3Gi',
    #     'limit_cpu': '2000m',
    #     'limit_memory': '4Gi'
    # }
)

# task3
task3 = KubernetesPodOperator(
    namespace='default',
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="task3-test",
    task_id="task3-task",
    get_logs=True,
    startup_timeout_seconds=600,
    dag=dag
    # resources={
    #     'request_cpu': '8000m',
    #     'request_memory': '24Gi',
    #     'limit_cpu': '8000m',
    #     'limit_memory': '32Gi'
    # }   
)

stress = KubernetesPodOperator(
    namespace='default',
    image="alpine:latest",
    cmds=["fulload() { dd if=/dev/zero of=/dev/null | dd if=/dev/zero of=/dev/null | dd if=/dev/zero of=/dev/null | dd if=/dev/zero of=/dev/null & }; fulload; read; killall dd
"],
    # arguments=[""],
    # labels={"foo": "bar"},
    name="stress-test",
    task_id="stress-task",
    get_logs=True,
    startup_timeout_seconds=600,
    dag=dag
    # resources={
    #     'request_cpu': '8000m',
    #     'request_memory': '24Gi',
    #     'limit_cpu': '8000m',
    #     'limit_memory': '32Gi'
    # }   
)

task1.set_upstream(start)
task2.set_upstream(task1)
task3.set_upstream(task2)
stress.set_upstream(task3)
