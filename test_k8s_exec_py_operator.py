from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

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

dag = DAG('test_k8s_exec_py_operator', default_args=default_args, schedule_interval=timedelta(minutes=10), catchup=False)

start = DummyOperator(task_id='run_this_first', dag=dag)

# specs of t2.small
small = PythonOperator(
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
    executor_config={
        "KubernetesExecutor":{
            'request_cpu': '1000m',
            'request_memory': '2Gi',
            'limit_cpu': '1000m',
            'limit_memory': '2Gi'}
    }
)

# specs of t2.medium
medium = PythonOperator(
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
    executor_config={
        "KubernetesExecutor":{
            'request_cpu': '2000m',
            'request_memory': '3Gi',
            'limit_cpu': '2000m',
            'limit_memory': '4Gi'}
    }
)

# specs of t2.2xlarge
twoxlarge = PythonOperator(
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
    executor_config={
        "KubernetesExecutor":{
            'request_cpu': '8000m',
            'request_memory': '24Gi',
            'limit_cpu': '8000m',
            'limit_memory': '32Gi'}
    }
)

small.set_upstream(start)
medium.set_upstream(small)
twoxlarge.set_upstream(medium)
