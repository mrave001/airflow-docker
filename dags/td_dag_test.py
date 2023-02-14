from datetime import datetime, timedelta, timezone
import time
import uuid
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
import json
from subprocess import Popen, PIPE
from functools import reduce
from airflow.operators.python_operator import PythonOperator
from operators.teradata import TeradataOperator
from airflow.operators.bash import BashOperator
from hooks.teradata import TeradataHook
import pandas as pd
import os
#import ptvsd

DEFAULT_ARGS = {
    'owner': 'FSA',
    'depends_on_past': False,
    'start_date': '2020-06-20',
    'email': [''],
    'email_on_failure': False,
    #'on_failure_callback': ErrorUtils.failure_callback,
    'trigger_rule': 'none_failed_or_skipped',
    'email_on_retry': False,
    'jdbc_conn_id': 'teradata',
    'params': {
        'jla_mart_schema': 'sandbox'
    }
}

dag = DAG('td-dag-test',  # set Dag name here
          default_args=DEFAULT_ARGS,
          catchup=False,
          description='teradata test',  # set description here
          schedule_interval=None)  # define schedule interval if needed.

start_step = DummyOperator(
    dag=dag,
    task_id="start_step"
)

# Local path variable for saving data
local_path = "/opt/airflow/data/myfirst.csv"
#local_path = "/Users/mravelo/Desktop/airflow_tutorial/myfirst.csv"

def get_data_func__(**kwargs) :
    """
    Moves data from Teradata to Snowflake
    :param kwargs: DAG context of current execution
    :type kwargs: dict
    """
    context = kwargs
    ti = context['ti']
    print("--- INFO: Move data process started ---")

    # ptvsd.enable_attach(address=('localhost', 5678), redirect_output=True)
    # ptvsd.wait_for_attach()
    
    # execute sql and load data into dataframe from Teradata
    td = TeradataHook(jdbc_conn_id=DEFAULT_ARGS['jdbc_conn_id'])
    df = td.get_pandas_df("SELECT * FROM Sandbox.acct_jla_ns_environments")
    print("we came back from Teradata")
    print("\n",df)
    
    print("--- INFO: Move data process complete ---")
    
    print("--- INFO: CSV file created ---")
    df.to_csv(local_path, header=False, index=False,quoting=1)
    return 

get_data = PythonOperator(
    task_id='get_data',
    provide_context=True,
    python_callable=get_data_func__,
    dag=dag
)

get_data << [start_step]