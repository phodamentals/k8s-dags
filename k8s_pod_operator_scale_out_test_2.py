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

dag = DAG('k8s_auto_scale_out_test_2', default_args=default_args, schedule_interval=timedelta(minutes=10), catchup=False)

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
    startup_timeout_seconds=300,
    dag=dag,
    is_delete_operator_pod=True,
    resources={
        'request_cpu': '250m',
        'request_memory': '250Mi',
        'limit_cpu': '250m',
        'limit_memory': '250mi'
    }
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
    startup_timeout_seconds=300,
    dag=dag,
    is_delete_operator_pod=True,
    resources={
        'request_cpu': '250m',
        'request_memory': '250Mi',
        'limit_cpu': '250m',
        'limit_memory': '250mi'
    }
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
    startup_timeout_seconds=300,
    dag=dag,
    is_delete_operator_pod=True,
    resources={
        'request_cpu': '250m',
        'request_memory': '250Mi',
        'limit_cpu': '250m',
        'limit_memory': '250mi'
    }
)

# task4
task4 = KubernetesPodOperator(
    namespace='default',
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="task4-test",
    task_id="task4-task",
    get_logs=True,
    startup_timeout_seconds=300,
    dag=dag,
    is_delete_operator_pod=True,
    resources={
        'request_cpu': '250m',
        'request_memory': '250Mi',
        'limit_cpu': '250m',
        'limit_memory': '250mi'
    }
)

# task5
task5 = KubernetesPodOperator(
    namespace='default',
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="task5-test",
    task_id="task5-task",
    get_logs=True,
    startup_timeout_seconds=300,
    dag=dag,
    is_delete_operator_pod=True,
    resources={
        'request_cpu': '250m',
        'request_memory': '250Mi',
        'limit_cpu': '250m',
        'limit_memory': '250mi'
    }
)

# task6
task6 = KubernetesPodOperator(
    namespace='default',
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="task6-test",
    task_id="task6-task",
    get_logs=True,
    startup_timeout_seconds=300,
    dag=dag,
    is_delete_operator_pod=True,
    resources={
        'request_cpu': '250m',
        'request_memory': '250Mi',
        'limit_cpu': '250m',
        'limit_memory': '250mi'
    }
)

# task7
task7 = KubernetesPodOperator(
    namespace='default',
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="task7-test",
    task_id="task7-task",
    get_logs=True,
    startup_timeout_seconds=300,
    dag=dag,
    is_delete_operator_pod=True,
    resources={
        'request_cpu': '250m',
        'request_memory': '250Mi',
        'limit_cpu': '250m',
        'limit_memory': '250mi'
    }
)

# task8
task8 = KubernetesPodOperator(
    namespace='default',
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="task8-test",
    task_id="task8-task",
    get_logs=True,
    startup_timeout_seconds=300,
    dag=dag,
    is_delete_operator_pod=True,
    resources={
        'request_cpu': '250m',
        'request_memory': '250Mi',
        'limit_cpu': '250m',
        'limit_memory': '250mi'
    }
)

# # task9
# task9 = KubernetesPodOperator(
#     namespace='default',
#     image="python:3.6",
#     cmds=["python","-c"],
#     arguments=["print('hello world')"],
#     labels={"foo": "bar"},
#     name="task9-test",
#     task_id="task9-task",
#     get_logs=True,
#     startup_timeout_seconds=300,
#     dag=dag,
#     is_delete_operator_pod=True,
#     resources={
#         'request_cpu': '250m',
#         'request_memory': '250Mi',
#         'limit_cpu': '250m',
#         'limit_memory': '250mi'
#     }
# )
# # task10
# task10 = KubernetesPodOperator(
#     namespace='default',
#     image="python:3.6",
#     cmds=["python","-c"],
#     arguments=["print('hello world')"],
#     labels={"foo": "bar"},
#     name="task10-test",
#     task_id="task10-task",
#     get_logs=True,
#     startup_timeout_seconds=300,
#     dag=dag,
#     is_delete_operator_pod=True,
#     resources={
#         'request_cpu': '250m',
#         'request_memory': '250Mi',
#         'limit_cpu': '250m',
#         'limit_memory': '250mi'
#     }
# )

# stress = KubernetesPodOperator(
#     namespace='default',
#     image="alpine:latest",
#     cmds=['ping 0 -c 1000'],
#     # cmds=["fulload() { dd if=/dev/zero of=/dev/null | dd if=/dev/zero of=/dev/null | dd if=/dev/zero of=/dev/null | dd if=/dev/zero of=/dev/null & }; fulload; read; killall dd"],
#     # arguments=[""],
#     # labels={"foo": "bar"},
#     name="stress-test",
#     task_id="stress-task",
#     get_logs=True,
#     startup_timeout_seconds=300,
#     dag=dag
#     # resources={
#     #     'request_cpu': '8000m',
#     #     'request_memory': '24Gi',
#     #     'limit_cpu': '8000m',
#     #     'limit_memory': '32Gi'
#     # }   
# )

task1.set_upstream(start)
task2.set_upstream(task1)
task3.set_upstream(task2)
task4.set_upstream(task3)
task5.set_upstream(task4)
task6.set_upstream(task5)
task7.set_upstream(task6)
task8.set_upstream(task7)
# task9.set_upstream(task8)
# task10.set_upstream(task9)
# stress.set_upstream(task10)
