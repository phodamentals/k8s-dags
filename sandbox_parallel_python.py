import airflow
from datetime import datetime, timedelta, time

import os

import logging
from airflow import configuration as conf
from airflow.models import DagBag, TaskInstance
from airflow import DAG, settings
from airflow.models import Variable

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.operators import SSHExecuteOperator
from airflow.contrib.hooks import SSHHook

from python_lib import helper_tools as dau

import random

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

main_dag_id = 'test_parallel_python_operator'
#dag = DAG(main_dag_id, default_args=default_args, schedule_interval=timedelta(minutes=60), catchup=False)
dag = DAG(main_dag_id, default_args=default_args, catchup=False)

ssh_conn = 'ssh_etl_dev_001'
ssh_hook = SSHHook(conn_id=ssh_conn)


### THIS IS A ONE OFF TEST ###
# task_id = "process_dataset_1153290"
# task = KubernetesPodOperator(
#     namespace='default',
#     image="demandanalytics/airflow-k8s-etl",
#     cmds=["python3 /git/etl/prep/job_run_prep_dataset.py 6 6 1153290"],
#     # arguments=["print('hello world')"],
#     # labels={"foo": "bar"},
#     name=task_id,
#     task_id=task_id,
#     get_logs=True,
#     startup_timeout_seconds=600,
#     dag=dag
# )

# def createDynamicETL(task_id, callableFunction, args):
#     bash_params = (
#         str(int(args['client_id']))
#         + ' ' + str(int(args['client_id']))
#         + ' ' + str(int(args['dataset_id']))
#         #+ ' 1'
#         #+ ' {{var.value.is_scheduled_' + dag_id + '}}'
#         #+ ' {{var.value.run_id_' + dag_id + '}}'
#     )

#     bash_command = 'python3 /git/etl/prep/job_run_prep_dataset.py ' + bash_params + ';'

    # ### THIS IS DYNAMIC CODE ###
    # task = KubernetesPodOperator(
    #     namespace='default',
    #     image="demandanalytics/airflow-k8s-etl",
    #     # cmds=[bash_command],
    #     # arguments=["print('hello world')"],
    #     # labels={"foo": "bar"},
    #     name=task_id,
    #     task_id=task_id,
    #     get_logs=True,
    #     startup_timeout_seconds=600,
    #     dag=dag
    #     # resources={
    #     #     'request_cpu': '1000m',
    #     #     'request_memory': '2Gi',
    #     #     'limit_cpu': '1000m',
    #     #     'limit_memory': '2Gi'
    #     # }
    # )

# task = SSHExecuteOperator(
#     task_id=task_id
#     , bash_command='sudo ' + bash_command
#     , ssh_hook=ssh_hook
#     , trigger_rule='all_done'
#     , dag = dag
#     , email_on_failure=True
#     , email='none@none.org'
# )

# return task


def resetTasksStatus(task_id, execution_date):
    logging.info("Resetting: " + task_id + " " + execution_date)

    dag_folder = conf.get('core', 'DAGS_FOLDER')
    dagbag = DagBag(dag_folder)
    check_dag = dagbag.dags[main_dag_id]
    session = settings.Session()

    my_task = check_dag.get_task(task_id)
    ti = TaskInstance(my_task, execution_date)
    state = ti.current_state()
    logging.info("Current state of " + task_id + " is " + str(state))
    ti.set_state(None, session)
    state = ti.current_state()
    logging.info("Updated state of " + task_id + " is " + str(state))

    return


start = DummyOperator(
    task_id='start',
    dag=dag
)

start_process_datasets = DummyOperator(
    task_id='start_process_datasets',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

def find_datasets():
    # datasets_ret = [
    #     {
    #         'task_id': 'process_dataset_1'
    #         , 'callableFunction': 'hello_world'
    #     }, {
    #         'task_id': 'process_dataset_2'
    #         , 'callableFunction': 'hello_world'
    #     }, {
    #         'task_id': 'process_dataset_3'
    #         , 'callableFunction': 'hello_world'
    #     }
    # ]

    project_id = 6
    filter_condition = "dataset_type_stage='raw' and is_deleted=false and project_id=" + str(project_id)
    filter_condition += " and datafeed_set_id=91400 and dataset_id >= 1152982 and dataset_id <= 1152985"
    # filter_condition += " and datafeed_set_id=91400 and dataset_id >= 1152982 and dataset_id <= 1152992"
    # filter_condition += " and dataset_id_child is null"

    df_datasets = dau.insert_dataframe_frompostgresqltable('etl', 'v_datasets', filter_condition=filter_condition)
    df_datasets.loc[:, 'task_id'] = df_datasets['dataset_id'].apply(lambda x: 'process_dataset_' + str(x))
    df_datasets.loc[:, 'callableFunction'] = 'hello_world'

    datasets_ret = df_datasets.to_dict('records')

    return datasets_ret


task_find_datasets = DummyOperator(
    task_id="find_datasets",
    # python_callable=hello_world,
    dag=dag,
    # provide_context=True,
    # op_args=[]
)

datasets_get = find_datasets()

#In this loop tasks are created for each table defined in the YAML file
# for dataset in datasets:
for dataset in datasets_get:
    #In our example, first step in the workflow for each table is to get SQL data from db.
    #Remember task id is provided in order to exchange data among tasks generated in dynamic way.
    process_dataset_task = createDynamicETL(
        dataset['task_id']
        , dataset['callableFunction']
        , dataset
        # , {'dataset_id': index}
        # , {'host': 'host', 'user': 'user', 'port': 'port', 'password': 'pass', 'dbname': configFile['dbname']}
    )

    # #Second step is upload data to s3
    # upload_to_s3_task = createDynamicETL(
    #     '{}-uploadDataToS3'.format(table), 'uploadDataToS3'
    #     , {'previous_task_id': '{}-getSQLData'.format(table), 'bucket_name' : configFile['bucket_name'], 'prefix': configFile['prefix']}
    # )

    #This is where the magic lies. The idea is that
    #once tasks are generated they should linked with the
    #dummy operators generated in the start and end tasks. 
    #Then you are done!
    start_process_datasets >> process_dataset_task
    # get_sql_data_task >> upload_to_s3_task

    process_dataset_task >> end

# # Below code prevents this bug: https://issues.apache.org/jira/browse/AIRFLOW-1460
# for i in range(int(variableValue)):
#     resetTasksStatus('{}-process_dataset'.format(i), str(kwargs['execution_date']))

start >> task_find_datasets >> start_process_datasets >> end