import datetime
import pandas as pd
import paramiko
import re
from setup.setup_kyriba_header import *
from datetime import datetime, timezone
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow import DAG
from subprocess import Popen, PIPE
from functools import reduce
from operators.teradata import TeradataOperator
#from dsp_airflow.teradata import TeradataOperator
from airflow.operators.bash import BashOperator
from hooks.teradata import TeradataHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import table, column, select, insert, update
from sqlalchemy.sql.schema import Table, Column
from sqlalchemy import MetaData
from sqlalchemy.types import Integer, String, TIMESTAMP, Date
import utils.Error as ErrorUtils
#import JLA_Airflow.utils.Error as ErrorUtils

DEFAULT_ARGS = {
    'owner': 'FSA',
    'depends_on_past': False,
    'start_date': '2020-10-20',
    'email': ['mravelo@groupon.com'],
    'email_on_failure': True,
    'on_failure_callback': ErrorUtils.failure_callback,
    'trigger_rule': 'none_failed_or_skipped',
    'email_on_retry': False,
    'slack_conn_id':'slack_connection',
    'slack_channel':'#fsa-alerts-dev',
    'kyriba_conn_id': 'kyriba_sftp',
    'http_conn_id': 'http_jla_services_root',
    'remote_path': '/out/',
    'jdbc_conn_id': 'teradata',
    'ssh_conn_id':'zing',
    'zing_downloads':'/kyriba/downloads/',
    'zing_archive': '/kyriba/archive/',
    'params': {
        'jla_mart_schema': 'sandbox'
    }
}

docs = """
<img src="https://raw.github.groupondev.com/fsa/FSA_Team/main/Logos/fsa_logo_sm.png" alt="FSA Team" style="float:right;margin-right:22px;margin-top:22px;" />
### JLA - Amex Settlement Load
<br />
#### Purpose
This DAG is responsible for the daily loading of data from Kyriba regarding Netsuite payment records and persisting that data accordingly into the 
acct_jla_ns_payment_updates table in Teradata. This job then calls the Groupon.JLA.Services.NetSuite service, which will update the payment 
cleared date inside of Netsuite. This job is critical to our process of updating the payment clear date on vendor payments and indicating to 
NetSuite users when the payment has actually cleared with the bank. 
#### Outputs
This DAG creates a table of payment records within the [acct_jla_ns_payment_updates] table for the JLA data mart.
<br />
#### Owner
For any questions or concerns, please contact [fsa@groupon.com](mailto:fsa@groupon.com).
"""

dag = DAG('jla-pipeline-kyriba-ns',  # set Dag name here
          default_args=DEFAULT_ARGS,
          catchup=False,
          description='Imports and processes kyriba data and calls jla service',  # set description here
          schedule_interval=None)  # define schedule interval if needed.

# set documentation
dag.doc_md = docs

print("started dag")

start_pipeline_kyriba_ns = DummyOperator(
    dag=dag,
    task_id="start_pipeline_kyriba_ns"
)

# bind metadata to teradata engine for crud operations
meta = MetaData()

def get_process_uuid__func(**kwargs):
    """
    Sets the process_id/process_status variables - generates new process_id uuid
    :param kwargs: DAG context of current execution
    """
    context = kwargs
    
    print("about to get the uuid")
    import uuid
    kwargs['ti'].xcom_push(key="process_id", value=str(uuid.uuid4()))
    print("got the uuid")
    kwargs['ti'].xcom_push(key="process_status", value="running")
    kwargs['ti'].xcom_push(key="slack_conn_id", value=DEFAULT_ARGS['slack_conn_id'])
    kwargs['ti'].xcom_push(key="slack_webhook_token", value=BaseHook.get_connection(DEFAULT_ARGS['slack_conn_id']).password)
    kwargs['ti'].xcom_push(key="slack_channel", value=DEFAULT_ARGS['slack_channel'])

get_process_uuid = PythonOperator(
    provide_context=True,
    python_callable=get_process_uuid__func,
    dag=dag,
    task_id="get_process_uuid"
)

merge_process_log__sql = """
    MERGE INTO {{ params.jla_mart_schema }}.ACCT_JLA_RUN_PROCESS process
    USING 
    (
        SELECT 
            '{{ task_instance.xcom_pull(key="process_id") }}' as id,
            '{{ dag.dag_id }}' as process_name,
            '{{ ts }}' as start_date,
            '@end_date' as end_date,
            '{{ task_instance.xcom_pull(key="process_status") }}' as status,
            '{{ task_instance.xcom_pull(key="error_code") if task_instance.xcom_pull(key="error_code") else '' }}' as error_code,
            '{{ task_instance.xcom_pull(key="error_desc") if task_instance.xcom_pull(key="error_desc") else '' }}' as error_desc
    ) as pend_process 
    ON process.id = pend_process.id 
    WHEN MATCHED THEN UPDATE 
        SET end_dt = pend_process.end_date,
            status = pend_process.status,
            error_code = pend_process.error_code,
            error_desc = pend_process.error_desc,
            update_dt = CURRENT_TIMESTAMP 
    WHEN NOT MATCHED THEN INSERT 
    (
        id,
        process_name,
        start_dt,
        status,
        insert_dt,
        update_dt
    )
    VALUES
    (
        pend_process.id,
        pend_process.process_name,
        pend_process.start_date,
        pend_process.status,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP  
    );
"""

create_process_log = TeradataOperator(
    sql=merge_process_log__sql.replace("@end_date", datetime.now(timezone.utc).isoformat()),
    dag=dag,
    task_id="create_process_log"
)

def download_payment_data():
    
    # Connect to Kyriba
    username_value = BaseHook.get_connection(DEFAULT_ARGS['kyriba_conn_id']).login
    password_value = BaseHook.get_connection(DEFAULT_ARGS['kyriba_conn_id']).password
    ftp_value = BaseHook.get_connection(DEFAULT_ARGS['kyriba_conn_id']).host
    ftp_port = BaseHook.get_connection(DEFAULT_ARGS['kyriba_conn_id']).port
    transport = paramiko.Transport(ftp_value, ftp_port)
    transport.connect(username = username_value, password = password_value)
    print("Connected to Kyriba")

    kyriba = paramiko.SFTPClient.from_transport(transport)
    
    # Connect to Zing
    sshconn = DEFAULT_ARGS["ssh_conn_id"]
    try:
        ssh = SSHHook(ssh_conn_id=sshconn) 
        zing_sshclient = ssh.get_conn().open_sftp()
        print("Connected to Zing")
       
        # Open files in Kyriba and put them in downloads folder in Zing 
        remotefiles = kyriba.listdir(DEFAULT_ARGS['remote_path'])
        print(f"Files found in Kyriba /out directory: {remotefiles}")
        for remotefile in remotefiles: 
            try:
                # open csv file object from Kyriba
                kyribafile = kyriba.open(f"{DEFAULT_ARGS['remote_path']}{remotefile}", "r")
                print(f"Opened file: {remotefile}")
                # put csv file object into Zing
                zing_sshclient.putfo(kyribafile, f"{DEFAULT_ARGS['zing_downloads']}{remotefile}")
                print(f"Put file: {remotefile}")
            except Exception as err:
                print("Could not connect to Kyriba")
                print(f"Error: {err}")
    
    finally:
        # Close Zing connection
        print('Closed connection to Zing')
        if zing_sshclient:
            zing_sshclient.close()
    
    # Close connection to Kyriba
    kyriba.close()
    transport.close()

    return

downloading_payment_files = PythonOperator(
    task_id='downloading_payment_files',
    provide_context=True,
    python_callable=download_payment_data,
    dag=dag
)

def process_kyriba_data_func(**kwargs) :
    """
    Stores Kyriba csv data in WIP directory, parse Kyriba files,
    persist data within Teradata and call JLA NS service 
    endpoint - UpdatePayments()
    """
    
    context = kwargs
    print(f"This is the Dag run_id: \n{context['run_id']}")
    
    # Connect to Zing
    sshconn = DEFAULT_ARGS["ssh_conn_id"]
    try:
        ssh = SSHHook(ssh_conn_id = sshconn) 
        zing_sshclient = ssh.get_conn().open_sftp()
        print("Connected to Zing")

        print("--- INFO: Kyriba data process started ---")

        # Creating dataframes for NS2 and NS3 csv files found in downloads
        downloads = zing_sshclient.listdir("/kyriba/downloads")
        for file in downloads:

            if "NS2" in file:
                zingfile = zing_sshclient.open(f"{DEFAULT_ARGS['zing_downloads']}{file}", "r")
                print("We found an NS2 file")
                NS2_df = pd.read_csv(zingfile)
                NS2_df["envmnt_value"] = "NS2"
                NS2_df["filename"] = str(file)

            if "NS3" in file:
                zingfile = zing_sshclient.open(f"{DEFAULT_ARGS['zing_downloads']}{file}", "r")
                print("We found an NS3 file")
                NS3_df = pd.read_csv(zingfile)
                NS3_df["envmnt_value"] = "NS3"
                NS3_df["filename"] = str(file)

        # Concatanating both dataframes along rows
        res = [NS2_df, NS3_df]
        concat_df = pd.concat(res, axis = 0, ignore_index = True)

        # Drops the empty rows from dataframe
        concat_df.dropna(inplace=True)


        # Filters the Reference column of the dataframe for only rows that contains "P"
        concat_df = concat_df[concat_df["Reference"].str.contains("P")]


        # Transform Reference column - Remove first two characters and replace any "P" with "/"
        reference = [str(r["Reference"][2:].replace("P","/")) for i, r in concat_df.iterrows()]

        # Convert payment_clear_date format
        concat_df["Transaction date"]=pd.to_datetime(concat_df["Transaction date"]).dt.strftime('%Y-%m-%d')

        # Creating dataframe for Teradata
        td_df = pd.DataFrame(columns = create_header_columns())

        # Initialing dataframe for Teradata table 
        td_df["payment_clear_date"] = concat_df["Transaction date"]
        td_df["process_id"] = str(context['ti'].xcom_pull(key='process_id'))
        td_df["payment_reference"] = reference
        td_df["envmnt_value"] = concat_df["envmnt_value"]
        td_df["filename"] = concat_df["filename"]
        td_df["insert_ts"] = datetime.utcnow()
        td_df["updated_ts"] = datetime.utcnow()
        td_df["created_by"] = "jla-pipeline-kyriba-ns"
        td_df["last_modified_by"] = "jla-pipeline-kyriba-ns"

        # Resetting index
        td_df = td_df.reset_index(drop=True)
    
    finally:
        # Close Zing connection
        print('Closed connection to Zing')
        if zing_sshclient:
            zing_sshclient.close() 
            
    td_username = BaseHook.get_connection(DEFAULT_ARGS['jdbc_conn_id']).login
    td_password = BaseHook.get_connection(DEFAULT_ARGS['jdbc_conn_id']).password
    td_host = BaseHook.get_connection(DEFAULT_ARGS['jdbc_conn_id']).host
    td_host = td_host.replace('jdbc:teradata://','')
    print(f'td_host is:\n {td_host}')

    td_engine = create_engine(f"teradatasql://{td_username}:{td_password}\
        @tdwd.snc1:1025/sandbox?cop=false", encoding = 'utf-8')
    
    meta.bind = td_engine
    
    try:
        conn = td_engine.connect()
    except Exception as err:
        print('Failed connecting' , err)
    
    #print('\nGetting ready to_sql')
    td_df.to_sql(name='acct_jla_ns_payment_updates', con=td_engine, index=False, if_exists='append', dtype=create_header_dtypes())
    
    conn.close()
    print('\nConnection closed')

    return 

process_kyriba_data = PythonOperator(
    task_id='process_kyriba_data',
    provide_context=True,
    python_callable=process_kyriba_data_func,
    dag=dag
)

update_payment_service_task = SimpleHttpOperator(
    task_id='update_payment_service',
    method='GET',
    endpoint="""Groupon.JLA.Services.NetSuite/NetsuiteService.svc/ProcessPaymentUpdates/{{ task_instance.xcom_pull(key="process_id")}}/{{ dag.dag_id }}""",
    dag=dag
)

end_pipeline_kyriba_ns  = DummyOperator(
    dag=dag,
    task_id="end_pipeline_kyriba_ns"
)

# define the workflow
get_process_uuid << [start_pipeline_kyriba_ns]
create_process_log << [get_process_uuid]
downloading_payment_files << [create_process_log]
process_kyriba_data<<[downloading_payment_files]
update_payment_service_task << [process_kyriba_data]
end_pipeline_kyriba_ns << [update_payment_service_task]


#get_process_uuid << [start_pipeline_kyriba_ns]
#create_process_log << [get_process_uuid]
#downloading_payment_files << [create_process_log]
#process_kyriba_data << [downloading_payment_files]
#update_payment_service_task << [downloading_payment_files]
#update_process_log << [update_payment_service_task]
#end_pipeline_kyriba_ns << [update_process_log]