from datetime import datetime, timedelta, timezone
import time
from functools import reduce
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.utils.dates import days_ago
from airflow import DAG
import json
import uuid
import hashlib 
import os, re
import shutil
import glob
import pandas
from io import StringIO
from contextlib import closing
from airflow.contrib.hooks.ssh_hook import SSHHook
# from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.contrib.hooks.sftp_hook import SFTPHook 
from zipfile import ZipFile
# import snowflake.connector
#from snowflake.sqlalchemy import URL
from sqlalchemy.types import String, Integer
from sqlalchemy import create_engine
import utils.Error as ErrorUtils
from airflow.utils.email import send_email_smtp
from hooks.teradata import TeradataHook

# debugger import
# import ptvsd

# default run_date is a two day lag (due to data timing it is safer to process on a lag to reduce amount of unmatched trxs)
run_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")

def failure_callback(context):
    """
    The function that will be executed on DAG failure.

    :param context: The context of the executed task.
    :type context: dict
    """

    context['task_instance'].xcom_push(key="error_code", value=context['task_instance'].dag_id)
    context['task_instance'].xcom_push(key="process_status", value="fail")

    try:
        context['task_instance'].xcom_push(key="error_desc", value=context['exception'])
    except Exception as ex:
        context['task_instance'].xcom_push(key="error_desc", value="Unable to convert exception details, see execution log for details: {log_url}".format(log_url=context['task_instance'].log_url))

    dagid = context['task_instance'].dag_id
    taskid = context['task_instance'].task_id
    failreason = str(context['exception'])
    execdate = context['execution_date']
    logurl = context['task_instance'].log_url
           
    message = ':ampelred: AIRFLOW TASK FAILURE :ampelred:\n' \
              '\n' \
              'DAG:                             {dag}\n' \
              'TASKS:                          {task}\n' \
              'EXECUTION TIME:      {exec_date}\n'  \
              'REASON:                       {reason}\n' \
              'LOG:                              {log_url}\n' \
        .format(dag=dagid,
                task=taskid,
                reason=failreason,
                exec_date=execdate,
                log_url=logurl)
    print(message)


    blocks = [
        {
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": ":exclamation:Airflow Failure:exclamation:",
				"emoji": True
			}
		},
		{
			"type": "divider"
		},
        {
			"type": "section",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "*DAG*"
				},
				{
					"type": "mrkdwn",
					"text": f"{dagid}"
				},
				{
					"type": "mrkdwn",
					"text": "*Task*"
				},
				{
					"type": "mrkdwn",
					"text": f"{taskid}"
				},
				{
					"type": "mrkdwn",
					"text": "*Reason*"
				},
				{
					"type": "mrkdwn",
					"text": f"{failreason}"
				},
				{
					"type": "mrkdwn",
					"text": "*Log*"
				},
				{
					"type": "mrkdwn",
					"text": f"<{logurl}|View Log>"
				}
			]
		},
		# {
		# 	"type": "section",
		# 	"text": {
		# 		"type": "mrkdwn",
		# 		"text": f"*DAG*\t\t\t\t{dagid}\n*Task*\t\t\t\t{taskid}\n*Reason*\t\t\t{failreason}\n*Log*\t\t\t\t<{logurl}|View Log>"
		# 	}
		# },
		{
			"type": "divider"
		},
		{
			"type": "context",
			"elements": [
				{
					"type": "mrkdwn",
					"text": "To assign this failure to someone, create a Git issue."
				}
			]
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": f"<https://github.groupondev.com/FSA/JLA/issues/new?assignees=&labels=bug&template=jla-bug-report.md&title=Airflow+%7C+{dagid}+{failreason[:40]}|:octocat::git: Create Issue>"
			}
		}
    ]

    slack_conn = context['task_instance'].xcom_pull(key="slack_conn_id")
    slack_webhook_token = context['task_instance'].xcom_pull(key="slack_webhook_token")
    slack_channel = context['task_instance'].xcom_pull(key="slack_channel")
    
    print(slack_conn)
    print(slack_webhook_token)
    print(slack_channel)
    
    failed_alert = SlackWebhookOperator(
        task_id='slack_fail_alert2',
        http_conn_id=slack_conn,
        webhook_token=slack_webhook_token,
        #message=message,
        blocks=blocks,
        channel=slack_channel,
        username='airflow',
        dag=context['dag'])

    failed_alert.execute(context=context)
    
    return


DEFAULT_ARGS = {
    'owner': 'FSA',
    'depends_on_past': False,
    'start_date': '2020-03-20',
    'email': '',
    'email_on_failure': False,
    'trigger_rule': 'none_failed_or_skipped',
    'email_on_retry': False,
    #'on_failure_callback': ErrorUtils.failure_callback,
    'on_failure_callback': failure_callback,
    'jdbc_conn_id': 'teradata',
    'ssh_conn_id':'zing',
    'slack_conn_id':'slack_connection',
    'slack_channel':'#fsa-alerts-dev', 
    'params': {
        'run_date': run_date,
        'reload': '0',
        'remote_dir':'/paymentech/',
        'local_dir':'/usr/local/fsa/tmp/paymentech/',
        'zip_passphrase': '1135C38H', 
        'deposits_record_type':'RACT0010',
        'deposits_fields': ['record_type','submission_date','pid_number','pid_short_name','submission_number','record_number','entity_type','entity_number','presentment_currency','merchant_order_id','rdfi_number','account_number','expiration_date','amount','mop','action_code','auth_date','auth_code','auth_response_code','trace_number','consumer_country_code','reserved','mcc','token_ind','interchange_qual_cd','durbin_regulated','interchange_unit_fee','interchange_face_prcnt','total_interchange_amt','total_assess_amt','other_debit_passthru_fee','file_id','file_date'],
        'deposits_datatypes': { "record_type":object, "submission_date":object, "rdfi_number": Integer, "auth_response_code":object, "trace_number":Integer, "consumer_country_code":object, "reserved":object, "token_ind":object },
        'deposits_staging_table':'acct_jla_paymentech_deposits_staging',
        'chargebacks_record_type':'RPDE0017D',
        'chargebacks_fields':['record_type','entity_type','entity_number','chargeback_amount','previous_partial','presentment_currency','chargeback_category','status_flag','sequence_number','merchant_order_id','account_number','reason_code','transaction_date','chargeback_date','activity_date','current_action','fee_amount','usage_code','mop_code','authorization_date','chargeback_due_date','ticket_no','potential_bundled_chargebacks','token_indicator','file_id','file_date'],
        'chargebacks_datatypes':{ "status_flag":object, "potential_bundled_chargebacks":object, "token_indicator":object, "ticket_no": object },
        'chargebacks_staging_table':'acct_jla_paymentech_chargebacks_staging',
        'fees_record_type':'RFIN0011',
        'fees_fields':['record_type','category','sub_category','entity_type','entity_number','funds_transfer_instr_num','secure_bank_acct_num','currency','fee_schedule','mop','interchange_qualification','fee_type_description','action_type','unit_quantity','unit_fee','amount','rate','total_charge','file_id','file_date'],
        'fees_datatypes':{ "record_type":object, "category":object, "sub_category":object, "entity_type":object, "entity_number":object, "funds_transfer_instr_num":object, "secure_bank_acct_num":object, "currency":object,"fee_schedule":object, "mop":object, "interchange_qualification":object, "fee_type_description":object, "action_type":object, "file_id":object },
        'fees_staging_table':'acct_jla_paymentech_fees_staging',
        'jla_mart_schema': 'DEV_DWH_MART_JLA',
        'groupondw_schema': 'DND_PRODUCTION.prod_groupondw'
    }
}

docs = """
<img src="https://raw.github.groupondev.com/fsa/FSA_Team/main/Logos/fsa_logo_sm.png" alt="FSA Team" style="float:right;margin-right:22px;margin-top:22px;" />
### JLA - File System Check / SSH verification

<br />
#### Purpose

This DAG is responsible for testing out the file system and verifiying connection to ssh (Zing).

#### Outputs

Outputs an email indicating file system and success of ssh connection.

<br />
#### Owner

For any questions or concerns, please contact [fsa@groupon.com](mailto:fsa@groupon.com).
"""


dag = DAG('jla-utils-config-check',  # set Dag name here
          default_args=DEFAULT_ARGS,
          catchup=False,
          # set description here
          description='JLA config check workflow',
          schedule_interval=None)  # define schedule interval if needed.


# set documentation
dag.doc_md = docs

def init__func(**kwargs):
    """
    Sets the slack connection/notification variables for the error utility
    :param kwargs: DAG context of current execution
    """
    kwargs['ti'].xcom_push(key="slack_conn_id", value=DEFAULT_ARGS['slack_conn_id'])
    kwargs['ti'].xcom_push(key="slack_webhook_token", value=BaseHook.get_connection(DEFAULT_ARGS['slack_conn_id']).password)
    kwargs['ti'].xcom_push(key="slack_channel", value=DEFAULT_ARGS['slack_channel'])

check_load_start = PythonOperator(
    provide_context=True,
    python_callable=init__func,
    dag=dag,
    task_id="check_load_start"
)


check_filesystem = BashOperator(
    task_id = 'check_filesystem',
    bash_command='ls -lah /usr/local/airflow/dags/JLA_Airflow/secrets/',
    dag = dag
)

check_filesystem_mkdir = BashOperator(
    task_id = 'check_filesystem_mkdir',
    bash_command='touch /usr/local/airflow/dags/JLA_Airflow/utils/wip/input.txt',
    dag = dag
)

def slack_block_notification__func(**kwargs):
    """
    The function that will be executed on DAG success.

    :param context: The context of the executed task.
    :type context: dict
    """
    context = kwargs
    
    td = TeradataHook(jdbc_conn_id=DEFAULT_ARGS['jdbc_conn_id'])
    tieout_df = td.get_pandas_df("select * from sandbox.acct_jla_etl_daily_tieout where process_id = sandbox.acct_jla_proc_id_parm_tmp.run_process_id and table_source IN ('ctx_opt','jla_collections') order by table_source desc")
    # calculate the variances
    tieout_df['total_variance']=tieout_df['transaction_amount'].diff()
    tieout_df['cash_variance']=tieout_df['cash_amount'].diff()
    tieout_df['points_variance']=tieout_df['points_amount'].diff()
    tieout_df['exchange_variance']=tieout_df['exchange_amount'].diff()
    variance_emoji = ":large_green_circle:" if tieout_df['total_variance'][1]==0 else ":red_circle:"
    # print(f"Cash diff {tieout_df['cash_variance'][1]}")
    # print(f"Points diff {tieout_df['points_variance'][1]}")
    # print(f"Exchange diff {tieout_df['exchange_variance'][1]}")

    jla_collections_df = tieout_df[tieout_df.table_source=='jla_collections']
    ctx_opt_df = tieout_df[tieout_df.table_source=='ctx_opt']
        
    tieout_exp_df = td.get_pandas_df("select process_id, parent_order_id, VARIANCE from sandbox.acct_jla_etl_daily_tieout_exp where process_id = sandbox.acct_jla_proc_id_parm_tmp.run_process_id")
    tieout_exceptions = []
    for ind in tieout_exp_df.index:
        # plug the values into the template string
        tieout_exceptions.append(f"Parent Order Id: {tieout_exp_df['parent_order_id'][ind]}\nVariance: {'${:,.2f}'.format(tieout_exp_df['VARIANCE'][ind])}\n\n")
        if ind == 10:
            break # grab top 10 (at most)

    # print("tieout exceptions:")
    print(''.join(tieout_exceptions))

    process_date = context['execution_date'].strftime("%Y-%m-%d")
    exceptions_count = len(tieout_exp_df.index)
    exceptions_top_count = exceptions_count if exceptions_count < 10 else 10
    exceptions_showing = f"Showing Top {exceptions_top_count} records..." if exceptions_count > 9 else f"Showing {exceptions_top_count} records..."
    exceptions_showing = "No tieout exception records... :tada:" if exceptions_count < 1 else exceptions_showing

    blocks = [
		{
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": f"JLA Mart ETL Complete for {process_date}"
			}
		},
		{
			"type": "divider"
		},
        {
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": f"*Daily Mart ETL Tieout* :scales::moneybag:"
			}
		},
        {
			"type": "section",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "JLA Collections"
				},
				{
					"type": "mrkdwn",
					"text": f"*{'${:,.2f}'.format(jla_collections_df['transaction_amount'][0])}*"
				},
				{
					"type": "mrkdwn",
					"text": f"_Cash_: {'${:,.2f}'.format(jla_collections_df['cash_amount'][0])}\n_Points_: {'${:,.2f}'.format(jla_collections_df['points_amount'][0])}\n_Exchange_: {'${:,.2f}'.format(jla_collections_df['exchange_amount'][0])}"
				},
				{
					"type": "mrkdwn",
					"text": " "
				}
			]
		},
		{
			"type": "section",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "OPT/CTX"
				},
				{
					"type": "mrkdwn",
					"text": f"*{'${:,.2f}'.format(ctx_opt_df['transaction_amount'][1])}*"
				},
				{
					"type": "mrkdwn",
					"text": f"_Cash_: {'${:,.2f}'.format(ctx_opt_df['cash_amount'][1])}\n_Points_: {'${:,.2f}'.format(ctx_opt_df['points_amount'][1])}\n_Exchange_: {'${:,.2f}'.format(ctx_opt_df['exchange_amount'][1])}"
				},
				{
					"type": "mrkdwn",
					"text": " "
				}
			]
		},
		{
			"type": "section",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "Variance"
				},
				{
					"type": "mrkdwn",
					"text": f"*{'${:,.2f}'.format(tieout_df['total_variance'][1])}*   {variance_emoji}"
				},
				{
					"type": "mrkdwn",
					"text": f"_Cash_: {'${:,.2f}'.format(tieout_df['cash_variance'][1])}\n_Points_: {'${:,.2f}'.format(tieout_df['points_variance'][1])}\n_Exchange_: {'${:,.2f}'.format(tieout_df['exchange_variance'][1])}"
				},
				{
					"type": "mrkdwn",
					"text": " "
				}
			]
		},
        {
            "type": "section",
			"text": {
				"type": "mrkdwn",
				"text": " "
			},
        },
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": f"*Transaction Tieout Exceptions ({exceptions_count})*"
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
					"text": f"{exceptions_showing} "
				}
			]
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": f"{''.join(tieout_exceptions)} "
			}
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": f"<https://fsa.group.on/JLA/ServiceStatus/ETLProcessLog/{jla_collections_df['process_id'][0]}|View Full Report>"
			}
		}
	]
    
    # call slack notifier app
    slack_conn = context['task_instance'].xcom_pull(key="slack_conn_id")
    slack_webhook_token = context['task_instance'].xcom_pull(key="slack_webhook_token")
    slack_channel = context['task_instance'].xcom_pull(key="slack_channel")
    
    # print(slack_conn)
    # print(slack_webhook_token)
    # print(slack_channel)
    # print(blocks)
    
    mart_load_alert = SlackWebhookOperator(
        task_id='jla_mart_load_alert',
        http_conn_id=slack_conn,
        webhook_token=slack_webhook_token,
        # message=message,
        blocks=blocks,
        channel=slack_channel,
        username='airflow',
        dag=context['dag'])

    mart_load_alert.execute(context=context)

    return

slack_block_notification = PythonOperator(
    provide_context=True,
    python_callable=slack_block_notification__func,
    dag=dag,
    task_id="slack_block_notification"
)


def zing_connect(**kwargs):

    context = kwargs

    sshconn = DEFAULT_ARGS["ssh_conn_id"]
    
    # ssh_client = None
    try:
        ssh = SSHHook(ssh_conn_id=sshconn)

        context['ti'].xcom_push(key="slack_conn_id", value=DEFAULT_ARGS['slack_conn_id'])
        context['ti'].xcom_push(key="slack_webhook_token", value=BaseHook.get_connection(DEFAULT_ARGS['slack_conn_id']).password)
        context['ti'].xcom_push(key="slack_channel", value=DEFAULT_ARGS['slack_channel'])

        ssh_client = ssh.get_conn().open_sftp()
        remote_files = ssh_client.listdir("/amex/2022-10-15/")

        for remotefile in remote_files:
            print("File found: " + remotefile)
            try:
                # open remote file object from vendor sftp
                settlementFile = ssh_client.open(f"/amex/2022-10-15/{remotefile}")
                print(f"Opened file: {remotefile}")                
                # put remote file object to zing sftp
                ssh_client.putfo(settlementFile, f"/amex/downloads/{remotefile}")
                print(f"Put file: {remotefile}")
            except Exception as err:
                print(f"Error: {err}")

                
    finally:
        print('finallly')
        if ssh_client:
            ssh_client.close()


check_ssh_zing = PythonOperator(
    task_id='check_ssh_zing',
    provide_context=True,
    python_callable=zing_connect,
    dag=dag
)


# def retrieve_paymentech_files__func(**kwargs):
#     """ 
#     Connects to Paymentech sftp directory based upon the run_date variable,
#     Downloads all files within directory, loops through each downloaded zip
#     file and then unzips each, calcs a sha256 checksum, and logs the file and sets xcom

#     :param kwargs: DAG context of current execution
#     """
#     # ptvsd.enable_attach(address=('localhost', 5678), redirect_output=True)
#     # ptvsd.wait_for_attach()

#     context = kwargs
    
#     # set connection ids from default args
#     sfconn = DEFAULT_ARGS["snowflake_conn_id"]
#     sshconn = DEFAULT_ARGS["ssh_conn_id"]

#     # get connection details
#     ssh = SSHHook(ssh_conn_id=sshconn)
    
#     # get paymentech file from sftp
#     with closing(ssh.get_conn().open_sftp()) as sftp_client:
#         # use run_date to download PSP files from run_date directory
#         sftp_dir = f"/paymentech/{context['params']['run_date']}/"
#         # create local processing directory using dag_run_id
#         runid = context['dag_run'].run_id
#         local_dir = f"/Users/bartarobinson/groupon_sftp/paymentech/{runid}/"
#         try:
#             os.mkdir(path=local_dir) 
#         except OSError as error:
#             print(error)
#         except: 
#             pass    

#         # add local_dir to xcom to allow cleanup
#         context['ti'].xcom_push(key="local_dir", value=local_dir)

#         try:
#             # get all files within the run_date directory
#             remote_files = sftp_client.listdir(sftp_dir)
#             for item in remote_files:
#                 try:
#                     rf = os.path.join(sftp_dir, item)
#                     lf = os.path.join(local_dir, item)
#                     print(f"Download { rf } to { lf }")
#                     sftp_client.get(rf, lf)
#                 except Exception as error:
#                     print(error)
                    
#         except Exception as error:
#             print(error)
                
#     # unzip all downloaded files in the local processing directory
#     compressed_files = filebrowser(path=local_dir, ext=".zip")
#     unzip_password = context['params']['zip_passphrase']
#     for zipfile in compressed_files:
#         # unzip file
#         with ZipFile(zipfile, 'r') as zipObj:
#             # Extract all the contents of zip file in current directory
#             zipObj.extractall(path=local_dir, pwd=bytes(unzip_password, encoding="ascii"))

#     files_to_load = []
#     data_files = filebrowser(path=local_dir, ext=".dfr")
#     # create jla file log objects for each file
#     for dfr_file in data_files:
#         sha256_hash = hashlib.sha256()
#         with open(dfr_file,"rb") as f:
#             # Read and update hash string value in blocks of 4K
#             for byte_block in iter(lambda: f.read(4096),b""):
#                 sha256_hash.update(byte_block)
#                 #print(sha256_hash.hexdigest())
        
#         checksum = sha256_hash.hexdigest()
#         #print(checksum)
        
#         # add record to JLA file log
#         # create uuid for the file
#         fileid = str(uuid.uuid4())
#         name = os.path.basename(dfr_file)
#         location = os.path.dirname(dfr_file)
#         try: 
#             filedate = datetime.strptime(name.split('.')[2], "%y%m%d").strftime("%Y-%m-%d")
#         except:
#             filedate = run_date
        
#         try:
#             dwh_hook = SnowflakeHook(snowflake_conn_id=sfconn, autocommit=True)
#             # update the sql merge statement with the current file properties
#             dwh_hook.run(sql=merge_file_log__sql.format(
#                 context['params']['jla_mart_schema'],
#                 fileid,
#                 name,
#                 location,
#                 checksum,
#                 context['params']['jla_mart_schema']),
#                 autocommit=True)
#             # add file to list of files to load into staging
#             files_to_load.append({ "file" : { "file_id": fileid, "file_date": filedate, "name": name, "location": location, "checksum": checksum } })
#         except Exception as error:
#             print(error)

#     # set files xcom so other tasks in dag can access the files to be loaded
#     context['ti'].xcom_push(key="files", value=files_to_load)
#     return
    
# def filebrowser(path="", ext=""):
#     """
#     Returns files with a specified extension, from specified path
#     :param path: absolute file path to search (default is empty)
#     :param ext: file extension to search (default is empty)
#     """
#     files = [os.path.abspath(f) for f in glob.glob(f"{path}*{ext}")]
#     # print(files)
#     return files

# retrieve_paymentech_files = PythonOperator(
#     provide_context=True,
#     python_callable=retrieve_paymentech_files__func,
#     dag=dag,
#     task_id="retrieve_paymentech_files"
# )

# email_no_records__message_ = """
#     <style>
#         body {
#             background-color: #FFF;
#             color: #333;
#         }
#     </style>
#     <p style="margin-bottom:28px;">
#         Unable to find Paymentech settlement data for {{ params.run_date }}, data files unavailable on zing.grouponinc.net/paymentech/{{ params.run_date}}/. Additional details 
#         may be available within the <a href={{ti.log_url}} target="_blank">Airflow workflow/task logs</a>.
#     </p>
#     <p>
#         Click to create a new <a href="https://jira.groupondev.com/secure/CreateIssue!default.jspa" target="_blank">JIRA</a> to request assistance with this issue from another team.
#     </p>
#     <br />
#     <br />
#     <img src="https://raw.github.groupondev.com/fsa/FSA_Team/main/Logos/fsa_logo_sm.png" alt="FSA Team"  />
# """

# email_no_records = EmailOperator(
#     dag=dag,
#     subject="Paymentech Settlement | {{ params.run_date }} | Data Not Found",
#     task_id="email_no_records",
#     html_content=email_no_records__message_,
#     to="barobinson@groupon.com" # TODO: update this email address before go-live/production
# )

def success_callback(**kwargs):
    """
    The function that will be executed on DAG success.

    :param context: The context of the executed task.
    :type context: dict
    """
    context = kwargs
    context['task_instance'].xcom_push(key="process_status", value="success")
    subject = f"JLA DAG Check | {context['execution_date']} | Success"
    message = 'Airflow Details:\n' \
              'DAG:     {}\n' \
              'DAG Run: {}\n' \
        .format(context['task_instance'].dag_id,
                context['dag_run'].run_id)
    print(f"Subject: {subject} \nMessage: {message}")

    # send notification
    send_email_smtp(DEFAULT_ARGS['email'], subject, message)
    
    return

check_load_end = PythonOperator(
    provide_context=True,
    python_callable=success_callback,
    dag=dag,
    task_id="check_load_end"
)


# DAG dependencies
[check_filesystem, check_filesystem_mkdir, check_ssh_zing, slack_block_notification] << check_load_start
check_load_end << [check_filesystem, check_filesystem_mkdir, check_ssh_zing, slack_block_notification]