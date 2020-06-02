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
from src.DropEveryTableOperator import DropEveryTableOperator

default_args = {
    'start_date': datetime(2018, 4, 1),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2018, 4, 2),
    'depends_on_past': False,
    'redshift_conn_id': 'redshift',
    'aws_login': 'aws_credentials',
    'catchup' : True,
    'socrata_id' : 'socrata', 
}

drop_dag = DAG('Drop everything in redshift',
          default_args=default_args,
          description='drop stuff',
          schedule_interval='30 23 * * *',
          max_active_runs=1,
)

drop_staging_loans = DropEveryTableOperator(
    task_id='drop_everything',
    dag=drop_dag,
)