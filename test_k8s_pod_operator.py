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

# specs of t2.small
small = KubernetesPodOperator(
    namespace='default',
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="small-test",
    task_id="small-task",
    get_logs=True,
    startup_timeout_seconds=600,
    dag=dag,
    resources={
        'request_cpu': '1000m',
        'request_memory': '2Gi',
        'limit_cpu': '1000m',
        'limit_memory': '2Gi'
    }
)

# specs of t2.medium
medium = KubernetesPodOperator(
    namespace='default',
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="medium-test",
    task_id="medium-task",
    get_logs=True,
    startup_timeout_seconds=600,
    dag=dag,
    # resources={
    #     'request_cpu': '2000m',
    #     'request_memory': '3Gi',
    #     'limit_cpu': '2000m',
    #     'limit_memory': '4Gi'
    # }
    resources={
        'request_cpu': '1500m',
        'request_memory': '2.5Gi',
        'limit_cpu': '1500m',
        'limit_memory': '2.5Gi'
    }
)

# specs of t2.2xlarge
twoxlarge = KubernetesPodOperator(
    namespace='default',
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="twoxlarge-test",
    task_id="twoxlarge-task",
    get_logs=True,
    startup_timeout_seconds=600,
    dag=dag,
    # resources={
    #     'request_cpu': '8000m',
    #     'request_memory': '24Gi',
    #     'limit_cpu': '8000m',
    #     'limit_memory': '32Gi'
    # }
    resources={
        'request_cpu': '1000m',
        'request_memory': '2Gi',
        'limit_cpu': '1000m',
        'limit_memory': '2Gi'
    }
)

small.set_upstream(start)
medium.set_upstream(small)
twoxlarge.set_upstream(medium)
