from datetime import datetime, timedelta, timezone
import time
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow import DAG
import json
from subprocess import Popen, PIPE
from functools import reduce
from airflow.operators.python_operator import PythonOperator
from operators.teradata import TeradataOperator
from hooks.teradata import TeradataHook
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import create_engine, table, column, select, insert, update
from sqlalchemy.sql.schema import Table, Column
from sqlalchemy import MetaData
from sqlalchemy.types import Integer, String, TIMESTAMP, Date
import utils.Error as ErrorUtils
import pandas
import os
#import ptvsd

os.chdir("/opt/airflow/dags/Mover/")

DEFAULT_ARGS = {
    'owner': 'FSA',
    'depends_on_past': False,
    'start_date': '2020-06-20',
    'email': [''],
    'email_on_failure': False,
    'on_failure_callback': ErrorUtils.failure_callback,
    'trigger_rule': 'none_failed_or_skipped',
    'email_on_retry': False,
    'region_name': 'us-west-2',
    'mssql_conn_id': 'gp',
    'jdbc_conn_id': 'teradata',
    'slack_conn_id':'slack_connection',
    'slack_channel':'#jla', 
    'params': {
        'jla_mart_schema': 'sandbox'
    }
}


docs = """
<img src="https://raw.github.groupondev.com/fsa/FSA_Team/main/Logos/fsa_logo_sm.png" alt="FSA Team" style="float:right;margin-right:22px;margin-top:22px;" />
### JLA - Mover - Move Tables from ~~Teradata to Snowflake~~ SQL Server to Teradata

<br />
#### Purpose

Move Teradata data into ~~Snowflake~~ SQL Server.

Additional details: <Confluence Link Here>

#### Outputs

This DAG queries data in Teradata and loads it into ~~Snowflake~~ SQL Server.

<br />
#### Owner

For any questions or concerns, please contact [fsa@groupon.com](mailto:fsa@groupon.com).
"""


dag = DAG('jla-data-mover',  # set Dag name here
          default_args=DEFAULT_ARGS,
          catchup=False,
          description='Loads data from Teradata tables into SQL Server',  # set description here
          schedule_interval=None)  # define schedule interval if needed.


# set documentation
dag.doc_md = docs

# bind metadata to teradata engine for crud operations
meta = MetaData()

start_jla_mover = DummyOperator(
    dag=dag,
    task_id="start_jla_mover"
)

def get_process_uuid__func(**kwargs):
    """
    Sets the process_id/process_status variables - generates new process_id uuid
    :param kwargs: DAG context of current execution
    """
    import uuid
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

table1_sql = [
    """
        SELECT top 10 * FROM sandbox.acct_jla_ns_environments;
    """
]

def jla_move_data_func__(**kwargs) :
    """
    Moves data from one data warehouse enviornment to another
    :param kwargs: DAG context of current execution
    :type kwargs: dict
    """
    context = kwargs
    ti = context['ti']
    print("--- INFO: Move data process started ---")

    # ptvsd.enable_attach(address=('localhost', 5678), redirect_output=True)
    # ptvsd.wait_for_attach()
    
    # execute sql and load data into dataframe
    sql = TeradataHook(jdbc_conn_id=DEFAULT_ARGS['jdbc_conn_id'])
    df = sql.get_pandas_df(table1_sql[0])
    # sql = MsSqlHook(mssql_conn_id=DEFAULT_ARGS['mssql_conn_id'])
    # df = sql.get_pandas_df(table1_sql[0])

    print('Dataframe: ')
    print(df)
    
    # call dataframe_to_sql and be sure to send a sqlalchemy engine for writing the data out
    td = TeradataHook(jdbc_conn_id=DEFAULT_ARGS['jdbc_conn_id'])
    # td_engine = td.get_sqlalchemy_engine(None)
    td_engine = create_engine('teradatasql://'+ BaseHook.get_connection(DEFAULT_ARGS['jdbc_conn_id']).login + ':' + BaseHook.get_connection(DEFAULT_ARGS['jdbc_conn_id']).password + '@tdwd.snc1:1025/sandbox?cop=false', encoding = 'utf-16')
    meta.bind = td_engine
    # persist data frame to data warehouse
    dataframe_to_sql(df,"acct_jla_data",td_engine,10000,False,"replace")
    
    print("--- INFO: Move data process complete ---")
    return

jla_move_data = PythonOperator(
    task_id='jla_move_data',
    provide_context=True,
    python_callable=jla_move_data_func__,
    dag=dag
)


# load_pay_on_redemption_data = SnowflakeOperator(
#     #snowflake_conn_id="{{ snowflake_connection_id }}",
#     sql = table1_sql,
#     dag=dag,
#     task_id="load_pay_on_redemption_data"
# )

# with open('Cont_Daily_Merchant_Payment_Reconciliation.sql', 'r') as cont_load_sql_file:
#     cont_load_sql = list(filter(None, cont_load_sql_file.read().split(';')))


# load_continuous_data = SnowflakeOperator(
#     #snowflake_conn_id="{{ snowflake_connection_id }}",
#     sql = cont_load_sql,
#     dag=dag,
#     task_id="load_continuous_data"
# )

# with open('Daily_Merchant_Payment_Reconciliation.sql', 'r') as pov_load_sql_file:
#     pov_load_sql = list(filter(None, pov_load_sql_file.read().split(';')))


# load_pay_on_view_data = SnowflakeOperator(
#     #snowflake_conn_id="{{ snowflake_connection_id }}",
#     sql = pov_load_sql,
#     dag=dag,
#     task_id="load_pay_on_view_data"
# )


update_process_log = TeradataOperator(
    sql=merge_process_log__sql.replace("@end_date", datetime.now(timezone.utc).isoformat()),
    dag=dag,
    task_id="update_process_log"
)


end_jla_mover  = DummyOperator(
    task_id='end_jla_mover',
    dag=dag
)


def dataframe_to_sql(df, table, db_engine, chunksize, index=True, if_exists="fail", method=None):
    """
    Calls dataframe to_sql method
    :param df: DataFrame to be persisted to sql database
    :type df: pandas DataFrame
    :param table: Tablename to write data frame to
    :type table: str
    :param db_engine: Database engine to be used by data frame (from SQLAlchemy create_engine)
    :type db_engine: SQLAlchemy Engine 
    :param index: Flag that instructs data frame to index the data with an auto incrementing number
    :type index: bool
    :param if_exists: How to behave if the table already exists.
                        fail: Raise a ValueError.
                        replace: Drop the table before inserting new values.
                        append: Insert new values to the existing table.
    :type if_exists: str
    :param method: Controls the SQL insertion clause used:
                        None : Uses standard SQL INSERT clause (one per row).
                        ‘multi’: Pass multiple values in a single INSERT clause.
                        callable with signature (pd_table, conn, keys, data_iter).
    :type method: str
    :param chunksize: Indicates number of transactions to insert in a single call
    :type chunksize: int
    """
    start_time = time.time()
    df.to_sql(name=table, con=db_engine, index=index, if_exists=if_exists, method=method, chunksize=chunksize)
    elapsedtime = time.time() - start_time
    records = len(df.index)
    recordstats = ""
    if records != 0:
        recordstats = f"... approximately {(elapsedtime/records)} seconds per record"

    print(f"--- STATS: {elapsedtime} seconds to write dataframe to sql ({records} records) {recordstats} ---")


get_process_uuid << [start_jla_mover]
create_process_log << [get_process_uuid]
jla_move_data << [create_process_log]
update_process_log << [jla_move_data]
end_jla_mover << [update_process_log]