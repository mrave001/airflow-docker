from datetime import datetime, timedelta, timezone
import time
from functools import reduce
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.teradata import TeradataOperator
#from dsp_airflow.teradata import TeradataOperator
from hooks.teradata import TeradataHook
#from dsp_airflow.teradata import TeradataHook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
from airflow import DAG
import json
import uuid
import hashlib 
import os, re
import shutil
import glob
import pandas
import teradatasql
import teradatasqlalchemy
from io import StringIO
from contextlib import closing
from airflow.contrib.hooks.ssh_hook import SSHHook
from zipfile import ZipFile
from sqlalchemy.types import String, Integer
from sqlalchemy import create_engine
from sqlalchemy import MetaData
import utils.Error as ErrorUtils
#import JLA_Airflow.utils.Error as ErrorUtils

# debugger import
# import ptvsd

# default run_date is a two day lag (due to data timing it is safer to process on a lag to reduce amount of unmatched trxs)
#run_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
run_date = '2022-11-22'

DEFAULT_ARGS = {
    'owner': 'FSA',
    'depends_on_past': False,
    'start_date': '2020-03-20',
    'email': ['barobinson@groupon.com'],
    'email_on_failure': False,
    'on_failure_callback': ErrorUtils.failure_callback,
    'trigger_rule': 'none_failed_or_skipped',
    'email_on_retry': False,
    'on_failure_callback': ErrorUtils.failure_callback,
    'region_name': 'us-west-2',
    'jdbc_conn_id': 'teradata',
    'slack_conn_id':'slack_connection',
    'slack_channel':'#fsa-alerts-dev',
    'ssh_conn_id':'zing',
    'params': {
        'run_date': run_date,
        'reload': '0',
        'remote_dir':'/paymentech/',
        'local_dir':'/Users/mravelo/fsa/tmp/paymentech/',
        'zip_passphrase': '1135C38H', 
        'deposits_record_type':'RACT0010',
        'deposits_fields': ['record_type','submission_date','pid_number','pid_short_name','submission_number','record_number','entity_type','entity_number','presentment_currency','merchant_order_id','rdfi_number','account_number','expiration_date','amount','mop','action_code','auth_date','auth_code','auth_response_code','trace_number','consumer_country_code','reserved','mcc','token_ind','interchange_qual_cd','durbin_regulated','interchange_unit_fee','interchange_face_prcnt','total_interchange_amt','total_assess_amt','other_debit_passthru_fee','file_id','file_date'],
        'deposits_datatypes': { "record_type": str("%.8s"), "submission_date": str("%.10s"), "rdfi_number": int, "auth_response_code": str("%.3s"), "trace_number": int, "consumer_country_code": str("%.12s"), "reserved": str("%.1s"), "token_ind": str("%.1s") },
        'deposits_staging_table':'i830_acct_jla_paymentech_deposits_staging',
        'chargebacks_record_type':'RPDE0017D',
        'chargebacks_fields':['record_type','entity_type','entity_number','chargeback_amount','previous_partial','presentment_currency','chargeback_category','status_flag','sequence_number','merchant_order_id','account_number','reason_code','transaction_date','chargeback_date','activity_date','current_action','fee_amount','usage_code','mop_code','authorization_date','chargeback_due_date','ticket_no','potential_bundled_chargebacks','token_indicator','file_id','file_date'],
        'chargebacks_datatypes':{ "status_flag":str("%.1s"), "potential_bundled_chargebacks":str("%.1s"), "token_indicator":str("%.1s"), "ticket_no": str("%.15s") },
        'chargebacks_staging_table':'i830_acct_jla_paymentech_chargebacks_staging',
        'fees_record_type':'RFIN0011',
        'fees_fields':['record_type','category','sub_category','entity_type','entity_number','funds_transfer_instr_num','secure_bank_acct_num','currency','fee_schedule','mop','interchange_qualification','fee_type_description','action_type','unit_quantity','unit_fee','amount','rate','total_charge','file_id','file_date'],
        'fees_datatypes':{ "record_type":str("%.8s"), "category":str("%.6s"), "sub_category":str("%.6s"), "entity_type":str("%.2s"), "entity_number":str("%.10s"), "funds_transfer_instr_num":str("%.10s"), "secure_bank_acct_num":str("%.10s"), "currency":str("%.3s"),"fee_schedule":str("%.10s"), "mop":str("%.2s"), "interchange_qualification":str("%.4s"), "fee_type_description":str("%.30s"), "action_type":str("%.1s"), "file_id":str("%.100s") },
        'fees_staging_table':'i830_acct_jla_paymentech_fees_staging',
        'jla_mart_schema': 'sandbox',
        'acct_jla_settlements': 'i831_acct_jla_settlements',
        'groupondw_schema': 'DND_PRODUCTION.prod_groupondw'
    }
}

docs = """
<img src="https://raw.github.groupondev.com/fsa/FSA_Team/main/Logos/fsa_logo_sm.png" alt="FSA Team" style="float:right;margin-right:22px;margin-top:22px;" />
### JLA - Paymentech Settlement Load

<br />
#### Purpose

This DAG is responsible for the daily loading of Paymentech settlement data into JLA. This job is critical to our O2C (Order-to-Cash) process, 
the data is loaded into our unified settlements table [acct_jla_settlements] and from there it can be matched to order data. The matching process
is handled via the <a href="http://fsa.grouponinc.net/JLA/ServiceStatus" target="_blank">JLA O2C Service</a>.

Additional details: <a href="https://confluence.groupondev.com/x/RUbpE" target="_blank">https://confluence.groupondev.com/x/RUbpE</a>

#### Outputs

This DAG creates Paymentech settlement records within the [acct_jla_settlements] table within the JLA data mart.

<br />
#### Owner

For any questions or concerns, please contact [fsa@groupon.com](mailto:fsa@groupon.com).
"""


dag = DAG('jla-psp-paymentech-load',  # set Dag name here
          default_args=DEFAULT_ARGS,
          catchup=False,
          # set description here
          description='JLA workflow for loading Paymentech settlement files for O2C (order-to-cash) processing, loads data for both USD and CAD',
          schedule_interval= None)  # define schedule interval if needed.


# set documentation
dag.doc_md = docs


paymentech_load_start = DummyOperator(
    dag=dag,
    task_id="paymentech_load_start"
)

def get_load_run_date__func(**kwargs):
    """
    Gets/sets current execution run_date variable
    :param kwargs: DAG context of current execution
    :type kwargs: dict
    """
    context = kwargs
    run_date = context['params']['run_date']

    kwargs['ti'].xcom_push(key="run_date", value=run_date)

get_load_run_date = PythonOperator(
    provide_context=True,
    python_callable=get_load_run_date__func,
    dag=dag,
    task_id="get_load_run_date"
)

def get_process_uuid__func(**kwargs):
    """
    Sets the process_id/process_status variables - generates new process_id uuid
    :param kwargs: DAG context of current execution
    """
    kwargs['ti'].xcom_push(key="process_id", value=str(uuid.uuid4()))
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

def retrieve_paymentech_files__func(**kwargs):
    """ 
    Connects to Paymentech sftp directory based upon the run_date variable,
    Downloads all files within directory, loops through each downloaded zip
    file and then unzips each, calcs a sha256 checksum, and logs the file and sets xcom

    :param kwargs: DAG context of current execution
    """
    # ptvsd.enable_attach(address=('localhost', 5678), redirect_output=True)
    # ptvsd.wait_for_attach()

    context = kwargs
    
    # set connection ids from default args
    tdconn = DEFAULT_ARGS["jdbc_conn_id"]
    sshconn = DEFAULT_ARGS["ssh_conn_id"]

    # get connection details
    ssh = SSHHook(ssh_conn_id=sshconn)
    
    # get paymentech file from sftp
    with closing(ssh.get_conn().open_sftp()) as sftp_client:
        # use run_date to download PSP files from run_date directory
        sftp_dir = f"/paymentech/{context['params']['run_date']}/"
        # create local processing directory using dag_run_id
        runid = context['dag_run'].run_id
        local_dir = f"{os.getcwd()}/dags/data/"
        try:
            os.mkdir(path=local_dir) 
        except OSError as error:
            print(error)
        except: 
            pass    

        # add local_dir to xcom to allow cleanup
        context['ti'].xcom_push(key="local_dir", value=local_dir)

        try:
            # get all files within the run_date directory
            remote_files = sftp_client.listdir(sftp_dir)
            for item in remote_files:
                try:
                    rf = os.path.join(sftp_dir, item)
                    lf = os.path.join(local_dir, item)
                    print(f"Download { rf } to { lf }")
                    sftp_client.get(rf, lf)
                except Exception as error:
                    print(error)
                    
        except Exception as error:
            print(error)
                
    # unzip all downloaded files in the local processing directory
    compressed_files = filebrowser(path=local_dir, ext=".zip")
    unzip_password = context['params']['zip_passphrase']
    print(compressed_files)
    for zipfile in compressed_files:
        # unzip file
        with ZipFile(zipfile, 'r') as zipObj:
            # Extract all the contents of zip file in current directory
            print(zipfile)
            zipObj.extractall(path=local_dir, pwd=bytes(unzip_password, encoding="ascii"))

    files_to_load = []
    data_files = filebrowser(path=local_dir, ext=".dfr")
    # create jla file log objects for each file
    for dfr_file in data_files:
        sha256_hash = hashlib.sha256()
        with open(dfr_file,"rb") as f:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: f.read(4096),b""):
                sha256_hash.update(byte_block)
                #print(sha256_hash.hexdigest())
        
        checksum = sha256_hash.hexdigest()
        print(checksum)
        
        # add record to JLA file log
        # create uuid for the file
        fileid = str(uuid.uuid4())
        name = os.path.basename(dfr_file)
        location = os.path.dirname(dfr_file)
        try: 
            filedate = datetime.strptime(name.split('.')[2], "%y%m%d").strftime("%Y-%m-%d")
        except:
            filedate = run_date
        
        files_to_load.append({ "file" : { "file_id": fileid, "file_date": filedate, "name": name, "location": location, "checksum": checksum } })
        
        # try:
        #     dwh_hook = TeradataHook(jdbc_conn_id=tdconn, autocommit=True)
        #     # update the sql merge statement with the current file properties
        #     dwh_hook.run(sql=merge_file_log__sql.format(
        #         context['params']['jla_mart_schema'],
        #         fileid,
        #         name,
        #         location,
        #         checksum,
        #         context['params']['jla_mart_schema']),
        #         autocommit=True)
        #     # add file to list of files to load into staging
        #     files_to_load.append({ "file" : { "file_id": fileid, "file_date": filedate, "name": name, "location": location, "checksum": checksum } })
        # except Exception as error:
        #     print(error)

    # set files xcom so other tasks in dag can access the files to be loaded
    context['ti'].xcom_push(key="files", value=files_to_load)
    return
    
def filebrowser(path="", ext=""):
    """
    Returns files with a specified extension, from specified path
    :param path: absolute file path to search (default is empty)
    :param ext: file extension to search (default is empty)
    """
    files = [os.path.abspath(f) for f in glob.glob(f"{path}*{ext}")]
    # print(files)
    return files

retrieve_paymentech_files = PythonOperator(
    provide_context=True,
    python_callable=retrieve_paymentech_files__func,
    dag=dag,
    task_id="retrieve_paymentech_files"
)

merge_file_log__sql = """
    MERGE INTO {}.ACCT_JLA_FILE filelog
    USING 
    (
        SELECT 
            '{}' as id,
            '{}' as name, 
            '{}' as location,
            ft.id AS file_type_id,
            '{}' as checksum,
            'Paymentech' as file_type_name 
        FROM {}.ACCT_JLA_FILE_TYPE ft
        WHERE ft."NAME" = file_type_name
    ) as file_stage
    ON filelog.id = file_stage.id
    WHEN NOT MATCHED THEN INSERT 
    (
        id,
        name,
        location,
        file_type_id,
        checksum,
        insert_ts,
        update_ts
    )
    VALUES
    (
        file_stage.id,
        file_stage.name,
        file_stage.location,
        file_stage.file_type_id,
        file_stage.checksum,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP  
    );
"""

staging_script = []
    
#with open(f"{os.getcwd()}/dags/script/", 'r') as sql_scripts:
#    staging_script = [list(filter(None, sql_scripts.read().split(';'))) for i in sql_scripts]

with open('dags/script/create_deposits_staging.sql', 'r') as stage_deposits_staging_file:
    #staging_script = [filter(None, i.read().split(';')) for i in stage_deposits_staging_file]
    lines = list(filter(None, stage_deposits_staging_file.read().split(';')))
    staging_script.append(lines)

with open('dags/script/create_fees_staging.sql', 'r') as stage_fees_staging_file:
    lines = list(filter(None, stage_fees_staging_file.read().split(';')))
    staging_script.append(lines)
    
with open('dags/script/create_chargebacks_staging.sql', 'r') as stage_chargebacks_staging_file:
    lines = list(filter(None, stage_chargebacks_staging_file.read().split(';')))
    staging_script.append(lines)


create_staging_tables = TeradataOperator(
    sql=staging_script,
    autocommit=True,
    dag=dag,
    task_id="create_staging_tables"
)

def load_staging__func(record_type="", table="", columns=[], datatypes={}, **kwargs):
    """
    Reads Paymentech files and parses out all data records and then loads them into staging
    :param record_type: indicates which records will be loaded into staging
    :param table: staging table name
    :param columns: array of column names for this record type
    :param datatypes: dictionary of data types
    :param kwargs: DAG context of current execution
    """
    
    # ptvsd.enable_attach(address=('localhost', 5678), redirect_output=True)
    # ptvsd.wait_for_attach()

    context = kwargs
    tdconn = DEFAULT_ARGS["jdbc_conn_id"]

    if not (all([record_type, table, columns, datatypes])) :
        # invalid parameters, error condition
        raise Exception('Invalid call to load_staging__func...must pass record_type (string), table (string), columns (list of strings), datatypes (list of strings).')

    # get list of files to process, read each one into staging table
    files_to_load = context['ti'].xcom_pull(key="files")
    
    # list to hold the records to load
    records = []

    # for each file, parse it and add a couple extra fields to help track this load
    for loadfile in files_to_load:
        source_file = os.path.join(loadfile['file']['location'], loadfile['file']['name']) 
        source_file_id = loadfile['file']['file_id']
        source_filedate = loadfile['file']['file_date']

        with open(source_file,"r") as rawfile:
            for input_record in rawfile:
                first_col=input_record.split('\t')[0]
                if first_col.strip() == record_type:
                   output_record = re.sub('"', '', input_record)
                   output_record = re.sub('\t"|"\t', '\t', output_record)
                   output_record = output_record.rstrip('\n') + '\t' + source_file_id + '\t' + source_filedate
                   records.append(output_record)

    # read parsed records into dataframe using column names and data types
    df = pandas.read_table(StringIO('\n'.join(records)),sep='\t', header=None, names=columns, engine='python', dtype=datatypes)
    
    # bind metadata to teradata engine for crud operations
    meta = MetaData()
    
    # Set up teradata connection credentials
    td_username = BaseHook.get_connection(DEFAULT_ARGS['jdbc_conn_id']).login
    td_password = BaseHook.get_connection(DEFAULT_ARGS['jdbc_conn_id']).password
    td_host = BaseHook.get_connection(DEFAULT_ARGS['jdbc_conn_id']).host
    td_host = td_host.replace('jdbc:teradata://','')
    print(f'td_host is:\n {td_host}')
    
    # Teradata engine created so that dataframe can persist data to staging table
    td_engine = create_engine(f"teradatasql://{td_username}:{td_password}\
        @tdwd.snc1:1025/sandbox?cop=false", encoding = 'utf-8')
    
    meta.bind = td_engine
    
    try:
        conn = td_engine.connect()
    except Exception as err:
        print('Failed connecting' , err)

    # Pandas dataframe can write itself directly to Teradata with the to_sql method, just have to ensure that index=False
    # using if_exists='replace' forces to_sql to drop the table if it exists and recreate it 
    start_time = time.time()
    df.to_sql(name=table, con=td_engine, if_exists='replace',index=False,method='multi', chunksize=10000) #dtype=sqldatatypes
    print("--- STATS: %s seconds to write dataframe to sql ---" % (time.time() - start_time))
    
    conn.close()
    print('\nConnection closed')
    
    return


load_deposits_staging = PythonOperator(
    op_kwargs={ "record_type": "{{ params.deposits_record_type }}", 
        "table": "{{ params.deposits_staging_table }}",
        "columns": DEFAULT_ARGS['params']['deposits_fields'],
        "datatypes": dict(DEFAULT_ARGS['params']['deposits_datatypes'])
    },
    provide_context=True,
    python_callable=load_staging__func,
    dag=dag,
    task_id="load_deposits_staging"
)


load_settlement_deposits__sql = [
    """
        UPDATE {{ params.jla_mart_schema }}.{{ params.acct_jla_settlements }} 
        SET valid = 0 
        WHERE 1=1 
            AND file_source = 'Paymentech' 
            AND action_code IN ('DP','RF')  
            --AND file_date = '{{ params.run_date }}' 
            AND file_date IN (SELECT DISTINCT STAGE.FILE_DATE FROM {{ params.jla_mart_schema }}.{{ params.deposits_staging_table }} STAGE) 
            AND 1 = {{ params.reload }};
    """,
    """
        INSERT INTO {{ params.jla_mart_schema }}.{{ params.acct_jla_settlements }}
        (FILE_SOURCE,FILE_DATE,RECORD_TYPE,SUBMISSION_DATE,PID_NUMBER,PID_SHORT_NAME,SUBMISSION_NUMBER,RECORD_NUMBER,ENTITY_TYPE,ENTITY_NUMBER,PRESENTMENT_CURRENCY,MERCHANT_ORDER_ID,RDFI_NUMBER,ACCOUNT_NUMBER,EXPIRATION_DATE,AMOUNT,MOP,ACTION_CODE,AUTH_DATE,AUTH_CODE,AUTH_RESPONSE_CODE,TRACE_NUMBER,CONSUMER_COUNTRY_CODE,MCC,INTERCHANGE_QUAL_CD,DURBIN_REGULATED,INTERCHANGE_UNIT_FEE,TOTAL_INTERCHANGE_AMT,TOTAL_ASSESS_AMT,OTHER_DEBIT_PASSTHRU_FEE,INSERT_TS,DESCRIPTION,SERVICE_FEE,FILENAME)
        SELECT
            'Paymentech' --FILE_SOURCE
            , STAGE.FILE_DATE
            , STAGE.RECORD_TYPE
            , TO_DATE(STAGE.SUBMISSION_DATE, 'MM/DD/YYYY') AS SUBMISSION_DATE
            , STAGE.PID_NUMBER
            , STAGE.PID_SHORT_NAME
            , STAGE.SUBMISSION_NUMBER
            , STAGE.RECORD_NUMBER
            , STAGE.ENTITY_TYPE
            , STAGE.ENTITY_NUMBER
            , STAGE.PRESENTMENT_CURRENCY
            , STAGE.MERCHANT_ORDER_ID
            , STAGE.RDFI_NUMBER
            , STAGE.ACCOUNT_NUMBER
            , STAGE.EXPIRATION_DATE
            , STAGE.AMOUNT
            , STAGE.MOP
            , STAGE.ACTION_CODE
            , TO_DATE(STAGE.AUTH_DATE, 'MM/DD/YYYY') AS AUTH_DATE
            , STAGE.AUTH_CODE
            , STAGE.AUTH_RESPONSE_CODE
            , STAGE.TRACE_NUMBER
            , STAGE.CONSUMER_COUNTRY_CODE
            , STAGE.MCC
            , STAGE.INTERCHANGE_QUAL_CD
            , STAGE.DURBIN_REGULATED
            , STAGE.INTERCHANGE_UNIT_FEE
            , STAGE.TOTAL_INTERCHANGE_AMT
            , STAGE.TOTAL_ASSESS_AMT
            , STAGE.OTHER_DEBIT_PASSTHRU_FEE
            , CURRENT_TIMESTAMP
            , ''
            , 0
            , STAGE.FILE_ID
        FROM {{ params.jla_mart_schema }}.{{ params.deposits_staging_table }} STAGE
        LEFT OUTER JOIN {{ params.jla_mart_schema }}.{{ params.acct_jla_settlements }} SETTLE ON 
            SETTLE.VALID = 1 AND 
            SETTLE.FILE_SOURCE = 'Paymentech' AND  
            SETTLE.FILE_DATE = STAGE.FILE_DATE AND 
            SETTLE.ACTION_CODE = STAGE.ACTION_CODE AND 
            SETTLE.MERCHANT_ORDER_ID = STAGE.MERCHANT_ORDER_ID 
        WHERE 1=1 AND 
            STAGE.MOP <> 'AX' AND 
            SETTLE.SETTLEMENT_ID IS NULL;
    """
]

load_settlement_deposits = TeradataOperator(
    sql=load_settlement_deposits__sql,
    dag=dag,
    task_id="load_settlement_deposits"
)

load_chargeback_staging = PythonOperator(
    op_kwargs={ "record_type": "{{ params.chargebacks_record_type }}", 
        "table": "{{ params.chargebacks_staging_table }}",
        "columns": DEFAULT_ARGS['params']['chargebacks_fields'],
        "datatypes": dict(DEFAULT_ARGS['params']['chargebacks_datatypes'])
    },
    provide_context=True,
    python_callable=load_staging__func,
    dag=dag,
    task_id="load_chargeback_staging"
)


load_settlement_chargebacks__sql = [
    """
        UPDATE {{ params.jla_mart_schema }}.{{ params.acct_jla_settlements }} 
        SET valid = 0 
        WHERE 1=1 
            AND file_source = 'Paymentech' 
            AND action_code = 'CB' 
            --AND file_date = '{{ params.run_date }}' 
            AND file_date IN (SELECT DISTINCT STAGE.FILE_DATE FROM {{ params.jla_mart_schema }}.{{ params.chargebacks_staging_table }} STAGE) 
            AND 1 = {{ params.reload }};
    """,
    """
        INSERT INTO {{ params.jla_mart_schema }}.{{ params.acct_jla_settlements }}
        (FILE_SOURCE,FILE_DATE,RECORD_TYPE,SUBMISSION_DATE,PID_NUMBER,PID_SHORT_NAME,SUBMISSION_NUMBER,RECORD_NUMBER,ENTITY_TYPE,ENTITY_NUMBER,PRESENTMENT_CURRENCY,MERCHANT_ORDER_ID,RDFI_NUMBER,ACCOUNT_NUMBER,EXPIRATION_DATE,AMOUNT,MOP,ACTION_CODE,AUTH_DATE,AUTH_CODE,AUTH_RESPONSE_CODE,TRACE_NUMBER,CONSUMER_COUNTRY_CODE,RESERVED,MCC,TOKEN_IND,INTERCHANGE_QUAL_CD,DURBIN_REGULATED,INTERCHANGE_UNIT_FEE,TOTAL_INTERCHANGE_AMT,TOTAL_ASSESS_AMT,OTHER_DEBIT_PASSTHRU_FEE,INSERT_TS,DESCRIPTION,SERVICE_FEE,FILENAME) 
        SELECT 'Paymentech' --FILE_SOURCE
                , STAGE.FILE_DATE
                , LEFT(STAGE.RECORD_TYPE,8)
                , TO_DATE(CHARGEBACK_DATE, 'MM/DD/YYYY') AS SUBMISSION_DATE
                , 0 --PID_NUMBER
                , '' --PID_SHORT_NAME
                , '' --SUBMISSION_NUMBER
                , 0 --RECORD_NUMBER
                , STAGE.ENTITY_TYPE
                , STAGE.ENTITY_NUMBER
                , STAGE.PRESENTMENT_CURRENCY
                , STAGE.MERCHANT_ORDER_ID
                , 0 --RDFI_NUMBER
                , STAGE.ACCOUNT_NUMBER
                , '' --EXPIRATION_DATE
                , STAGE.CURRENT_ACTION --AMOUNT
                , STAGE.MOP_CODE
                ,'CB' --ACTION_CODE
                , NULL --AUTH_DATE
                , '' --AUTH_CODE
                , '' --AUTH_RESPONSE_CODE
                , 0 --TRACE_NUMBER
                , '' --CONSUMER_COUNTRY_CODE
                , '' --RESERVED
                , 0 --MCC
                , '' --TOKEN_IND
                , '' --INTERCHANGE_QUAL_CODE
                , '' --DURBIN_REGULATED
                , 0 --INTERCHANGE_UNIT_FEE
                , 0 --TOTAL_INTERCHANGE_AMT
                , 0 --TOTAL_ASSESS_AMT
                , 0 --OTHER_DEBIT_PASSTHRU_FEE
                , CURRENT_TIMESTAMP --INSERT_TS
                , ''
                , 0
                , STAGE.FILE_ID
        FROM {{ params.jla_mart_schema }}.{{ params.chargebacks_staging_table }} STAGE
        LEFT OUTER JOIN {{ params.jla_mart_schema }}.{{ params.acct_jla_settlements }} SETTLE ON 
            SETTLE.VALID = 1 
            AND SETTLE.FILE_SOURCE = 'Paymentech' 
            AND SETTLE.FILE_DATE = STAGE.FILE_DATE 
            AND SETTLE.ACTION_CODE = 'CB' 
            AND SETTLE.MERCHANT_ORDER_ID = STAGE.MERCHANT_ORDER_ID 
            AND SETTLE.AMOUNT = STAGE.CURRENT_ACTION
        WHERE 1=1 
            AND CHARGEBACK_CATEGORY IN ('REPR','RECD','PARREP','ADJPDE','OPAACCEPT')
            AND SETTLE.SETTLEMENT_ID IS NULL;
    """
]

load_settlement_chargebacks = TeradataOperator(
    sql=load_settlement_chargebacks__sql,
    dag=dag,
    task_id="load_settlement_chargebacks"
)

load_fee_staging = PythonOperator(
    op_kwargs={ "record_type": "{{ params.fees_record_type }}", 
        "table": "{{ params.fees_staging_table }}",
        "columns": DEFAULT_ARGS['params']['fees_fields'],
        "datatypes": dict(DEFAULT_ARGS['params']['fees_datatypes'])
    },
    provide_context=True,
    python_callable=load_staging__func,
    dag=dag,
    task_id="load_fee_staging"
)


load_settlement_fees__sql = [
    """
        UPDATE {{ params.jla_mart_schema }}.{{ params.acct_jla_settlements }} 
        SET valid = 0 
        WHERE 1=1 
            AND file_source = 'Paymentech' 
            AND action_code = 'SC' 
            --AND file_date = '{{ params.run_date }}' 
            AND file_date IN (SELECT DISTINCT STAGE.FILE_DATE FROM {{ params.jla_mart_schema }}.{{ params.fees_staging_table }} STAGE) 
            AND 1 = {{ params.reload }};
    """,
    """
        INSERT INTO {{ params.jla_mart_schema }}.{{ params.acct_jla_settlements }} 
        (FILE_SOURCE,FILE_DATE,RECORD_TYPE,SUBMISSION_DATE,PID_NUMBER,PID_SHORT_NAME,SUBMISSION_NUMBER,RECORD_NUMBER,ENTITY_TYPE,ENTITY_NUMBER,PRESENTMENT_CURRENCY,MERCHANT_ORDER_ID,RDFI_NUMBER,ACCOUNT_NUMBER,EXPIRATION_DATE,AMOUNT,MOP,ACTION_CODE,AUTH_DATE,AUTH_CODE,AUTH_RESPONSE_CODE,TRACE_NUMBER,CONSUMER_COUNTRY_CODE,RESERVED,MCC,TOKEN_IND,INTERCHANGE_QUAL_CD,DURBIN_REGULATED,INTERCHANGE_UNIT_FEE,TOTAL_INTERCHANGE_AMT,TOTAL_ASSESS_AMT,OTHER_DEBIT_PASSTHRU_FEE,INSERT_TS,DESCRIPTION,SERVICE_FEE,FILENAME)
        SELECT 
            'Paymentech'
            ,STAGE.FILE_DATE
            ,STAGE.RECORD_TYPE
            ,STAGE.FILE_DATE
            ,0
            ,''
            ,''
            ,0
            ,STAGE.ENTITY_TYPE
            ,STAGE.ENTITY_NUMBER
            ,STAGE.CURRENCY
            ,''
            ,0
            ,STAGE.SECURE_BANK_ACCT_NUM
            ,''
            ,STAGE.AMOUNT
            ,STAGE.MOP
            ,'SC'
            ,NULL
            ,''
            ,''
            ,0
            ,''
            ,''
            ,0
            ,''
            ,''
            ,''
            ,0
            ,0
            ,0
            ,0
            ,CURRENT_TIMESTAMP
            ,STAGE.FEE_TYPE_DESCRIPTION
            ,STAGE.TOTAL_CHARGE
            ,STAGE.FILE_ID
        FROM {{ params.jla_mart_schema }}.{{ params.fees_staging_table }} STAGE 
        LEFT OUTER JOIN {{ params.jla_mart_schema }}.{{ params.acct_jla_settlements }} SETTLE ON 
            SETTLE.VALID = 1 
            AND SETTLE.FILE_SOURCE = 'Paymentech' 
            AND SETTLE.FILE_DATE = STAGE.FILE_DATE 
            AND SETTLE.ACTION_CODE = 'SC' 
            AND SETTLE.PRESENTMENT_CURRENCY = STAGE.CURRENCY 
            AND ZEROIFNULL(SETTLE.ENTITY_NUMBER) = ZEROIFNULL(STAGE.ENTITY_NUMBER) 
            AND COALESCE(SETTLE.ENTITY_TYPE, '') = COALESCE(STAGE.ENTITY_TYPE, '') 
            AND COALESCE(SETTLE.ACCOUNT_NUMBER, '') = COALESCE(STAGE.SECURE_BANK_ACCT_NUM, '') 
            AND COALESCE(SETTLE.MOP,'') = COALESCE(STAGE.MOP,'') 
            AND ZEROIFNULL(SETTLE.AMOUNT) = ZEROIFNULL(STAGE.AMOUNT) 
            AND ZEROIFNULL(SETTLE.SERVICE_FEE) = ZEROIFNULL(STAGE.TOTAL_CHARGE) 
            AND COALESCE(SETTLE.DESCRIPTION,'') = COALESCE(STAGE.FEE_TYPE_DESCRIPTION,'') 
        WHERE SETTLE.SETTLEMENT_ID IS NULL;
    """
]

load_settlement_fees = TeradataOperator(
    sql=load_settlement_fees__sql,
    dag=dag,
    task_id="load_settlement_fees"
)

def has_records__branch__func(conditions, true_route, false_route, **kwargs):
    """
    Record check, if no records are found then we want to send an alert message over GChat...otherwise we'll load the data 
    into staging table(s) and continue with the process of loading the unified settlements table.
    :param conditions: evaluation/check conditions array
    :param true_route: branch to execute if result is true
    :param false_route: branch to execute if result is false
    :param kwargs: DAG context of current execution
    """

    context = kwargs
    res = reduce(lambda x, y: x and y, [
                 bool(eval(x)) for x in conditions], True)
    if res:
        # records not found, update process status so that it can be logged as a failure
        context['ti'].xcom_push(key="process_status", value="fail - no records")
        return true_route
    else:
        return false_route

has_records__branch = BranchPythonOperator(
    op_kwargs={"true_route": ["slack_no_records_exist"], "false_route": ["create_staging_tables"], "conditions": [
        " ( len({{ task_instance.xcom_pull(key='files') }}) == 0 ) "]},
    python_callable=has_records__branch__func,
    provide_context=True,
    dag=dag,
    task_id="has_records__branch"
)

def slack_no_records__func(**kwargs):
    context = kwargs
    run_date = context['ti'].xcom_pull(key="run_date")
    logurl = context['task_instance'].log_url

    blocks = [
    	{
    		"type": "header",
    		"text": {
    			"type": "plain_text",
    			"text": f"Paymentech PSP settlement data missing for {run_date} "
        }
    	},
    	{
    		"type": "divider"
    	},
    	{
    		"type": "section",
    		"text": {
    			"type": "mrkdwn",
    			"text": f"Unable to find Paymentech settlement data for {run_date}, not available in Teradata table [Paymentech_Settlement_Report]. Additional details may be available within the Airflow workflow/task <{logurl}|log>."
    		}
    	},
    	{
    		"type": "divider"
    	},
    	{
    		"type": "context",
    		"elements": [
    			{
    				"type": "mrkdwn",
    				"text": "To request assistance from another team, create a JIRA issue."
    			}
    		]
    	},
    	{
    		"type": "section",
    		"text": {
    			"type": "mrkdwn",
    			"text": "<https://jira.groupondev.com/secure/CreateIssue!default.jspa|:jira: Create JIRA Issue>"
    		}
    	}
    ]

    # NOTE: Sends a Slack message when no records are found using the slack integration address
    slack_no_records = SlackWebhookOperator(
        task_id ='slack_no_records_Paymentech_alert',
        http_conn_id=DEFAULT_ARGS['slack_conn_id'],
        webhook_token=BaseHook.get_connection(DEFAULT_ARGS['slack_conn_id']).password,
        blocks=blocks,
        channel=DEFAULT_ARGS['slack_channel'],
        username='airflow',
        dag=context['dag'])
    
    slack_no_records.execute(context=context)
    
    return

slack_no_records_exist = PythonOperator(
    provide_context=True,
    python_callable=slack_no_records__func,
    dag=dag,
    task_id="slack_no_records_exist")


update_process_log = TeradataOperator(
    sql=merge_process_log__sql.replace("@end_date", datetime.now(timezone.utc).isoformat()),
    dag=dag,
    task_id="update_process_log"
)


def paymentech_load_cleanup__func(**kwargs):
    """
    Performs final job cleanup, removes any local directories created during execution.
    :param kwargs: DAG context of current execution
    :type kwargs: dict
    """
    context = kwargs
    local_dir = context['ti'].xcom_pull(key="local_dir")

    try:
        shutil.rmtree(local_dir, ignore_errors=True)
    except Exception as error:
        print(error)


paymentech_load_cleanup = PythonOperator(
    provide_context=True,
    python_callable=paymentech_load_cleanup__func,
    dag=dag,
    task_id="paymentech_load_cleanup"
)


paymentech_load_end = DummyOperator(
    dag=dag,
    task_id="paymentech_load_end"
)


# DAG dependencies
get_load_run_date << [paymentech_load_start]
get_process_uuid << [get_load_run_date]
create_process_log << [get_process_uuid]
retrieve_paymentech_files << [create_process_log]
create_staging_tables << [has_records__branch]
load_deposits_staging << [create_staging_tables]
load_chargeback_staging << [create_staging_tables]
load_fee_staging << [create_staging_tables]
load_settlement_deposits << [load_deposits_staging]
load_settlement_chargebacks << [load_chargeback_staging]
load_settlement_fees << [load_fee_staging]
has_records__branch << [retrieve_paymentech_files]
slack_no_records_exist << [has_records__branch]
update_process_log << [
    load_settlement_deposits,
    load_settlement_chargebacks,
    load_settlement_fees,
    slack_no_records_exist
]
paymentech_load_cleanup << [update_process_log]
paymentech_load_end << [paymentech_load_cleanup]