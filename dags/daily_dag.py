from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging
from helper.sql_queries import fact, staging_tables, dim_tables, bridge_link
from src.downloaddata_operator import DownloadDataOperator
from src.uploaddata_operator import UploadDataOperator
from src.StageToRedshiftOperator import StageToRedshiftOperator
from src.ManipulateInventoryOperator import ManipulateInventoryOperator
from src.LoadDataOperator import LoadDataOperator
from src.DropTableOperator import DropTableOperator


default_args = {
    'start_date': datetime(2018, 4, 1),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2018, 5, 31),
    'depends_on_past': False,
    'redshift_conn_id': 'redshift',
    'aws_login': 'aws_credentials',
    'catchup' : True,
    'socrata_id' : 'socrata', 
}

daily_dag = DAG('Daily_DAG',
          default_args=default_args,
          description='Daily load loans and time_dim into data warehouse',
          schedule_interval='30 23 * * *',
          max_active_runs=1,
        )
start_operator_daily = DummyOperator(task_id='Begin_daily_execution',  dag=daily_dag)

download_loans_daily = DownloadDataOperator(
    task_id="download_loans",
    db_name ="loans_db",
    params= {'where':'checkoutdatetime > "{{ prev_ds }}" and checkoutdatetime <= "{{ ds }}"',\
        'select':"""id, checkoutyear,bibnumber, itembarcode, itemtype,collection, callnumber, itemtitle, subjects, checkoutdatetime"""},
    date= "{{ ds }}" ,
    file_path = "/home/jon/Downloads/Data/Loans/",
    dag = daily_dag)

upload_loans_to_s3_daily = UploadDataOperator(
    task_id="upload_loans",
    aws_id = "aws_credentials",
    date= "{{ ds }}" ,
    file_path = "/home/jon/Downloads/Data/Loans/loans_db_{}.csv",
    bucket_name = 'seattle-test',
    dag = daily_dag)

stage_loans_to_redshift = StageToRedshiftOperator(
    task_id="Stage_loans",
    dag=daily_dag,
    target_table = "staging_loans",
    s3_bucket = "seattle-test",
    s3_key = "loans_db_{{ds}}.csv",
    create_statement = staging_tables.create_loans_stg)

load_fact_table = LoadDataOperator(
    task_id='Load_fact_loan_table',
    dag=daily_dag,
    table = "fact_loans",
    sql_stmt_create = fact.create_fact_loans,
    sql_stmt_load = fact.insert_fact_loans,
    truncate_existing_table = False)

load_time_dim = LoadDataOperator(
    task_id='Load_dim_time_table',
    dag=daily_dag,
    table = "dim_time",
    sql_stmt_create = dim_tables.create_time_dim,
    sql_stmt_load = dim_tables.insert_time_dim,
    truncate_existing_table = False)

drop_staging_loans = DropTableOperator(
    task_id='drop_staging_loans',
    dag=daily_dag,
    table = "staging_loans"
)

start_operator_daily >> download_loans_daily >> upload_loans_to_s3_daily >> stage_loans_to_redshift >> [load_fact_table, load_time_dim] 
load_fact_table >> drop_staging_loans
