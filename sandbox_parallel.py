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

import random

default_args = {
    'owner': 'kevin@demandanalytics.io',
    'start_date': datetime.utcnow() - timedelta(days=1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

main_dag_id = 'sandbox_parallel'
dag = DAG(main_dag_id, default_args=default_args)

def hello_world(dataset_id, **kwargs):

    print('Hello dataset #' + str(dataset_id) + "!")

    return

# def createDynamicETL(task_id, callableFunction, args):
#     task = PythonOperator(
#         task_id = task_id,
#         provide_context=True,
#         #Eval is used since the callableFunction var is of type string
#         #while the python_callable argument for PythonOperators only receives objects of type callable not strings.
#         python_callable = eval(callableFunction),
#         op_kwargs = args,
#         xcom_push = True,
#         dag = dag,
#     )

def createDynamicETL(task_id, callableFunction, args):
    task = KubernetesPodOperator(
        task_id = task_id,
        provide_context=True,
        #Eval is used since the callableFunction var is of type string
        #while the python_callable argument for PythonOperators only receives objects of type callable not strings.
        python_callable = eval(callableFunction),
        op_kwargs = args,
        xcom_push = True,
        dag = dag,
    )

    return task

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

def find_datasets(*args, **kwargs):
    #Extract table names and fields to be processed
    num_datasets = random.randint(1, 20)
    datasets = []

    print(num_datasets)

    # for i in range(num_datasets):
    #     datasets.append(i + 1)

    # print(datasets)

    # You can set this value dynamically e.g., from a database or a calculation
    # dynamicValue = 3

    variableValue = Variable.get("datasets_test")
    logging.info("Current datasets_test value is " + str(variableValue))

    logging.info("Setting the Airflow Variable datasets_test to " + str(num_datasets))
    os.system('airflow variables --set datasets_test ' + str(num_datasets))

    variableValue = Variable.get("datasets_test")
    logging.info("Current datasets_test value is " + str(variableValue))


    # Below code prevents this bug: https://issues.apache.org/jira/browse/AIRFLOW-1460
    for i in range(int(variableValue)):
        resetTasksStatus('{}-process_dataset'.format(i), str(kwargs['execution_date']))

# find_datasets = PythonOperator(
find_datasets = KubernetesPodOperator(
    task_id="find_datasets",
    python_callable=find_datasets,
    dag=dag,
    provide_context=True,
    op_args=[]
)

datasets = Variable.get("datasets_test")
logging.info("The current datasets_test value is " + str(datasets))

#In this loop tasks are created for each table defined in the YAML file
# for dataset in datasets:
for index in range(int(datasets)):
    #In our example, first step in the workflow for each table is to get SQL data from db.
    #Remember task id is provided in order to exchange data among tasks generated in dynamic way.
    process_dataset_task = createDynamicETL(
        '{}-process_dataset'.format(index)
        , 'hello_world'
        , {'dataset_id': index}
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

start >> find_datasets >> start_process_datasets >> end