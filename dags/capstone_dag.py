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


default_args = {
    'start_date': datetime(2018, 3, 30),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2018, 5, 30),
    'depends_on_past': False,
    'redshift_conn_id': 'redshift',
    'aws_login': 'aws_credentials',
    'catchup' : True,
    'socrata_id' : 'socrata', 
}

monthly_dag = DAG('Monthly_DAG',
          default_args=default_args,
          description='Monthly load codes and dim into data warehouse',
          schedule_interval='0 0 1 * *',
          max_active_runs=1,
        )
start_operator = DummyOperator(task_id='Begin_monthly_execution',  dag=monthly_dag)

download_code = DownloadDataOperator(
    task_id="download_code",
    db_name ="code_db",
    params= {'select':"""code,description,code_type,format_group,format_subgroup,category_group,category_subgroup,age_group"""},
    date= "{{ ds }}" ,
    file_path = "/home/jon/Downloads/Data/Code/",
    dag = monthly_dag)

upload_code_to_s3 = UploadDataOperator(
    task_id="upload_code",
    aws_id = "aws_credentials",
    date= "{{ ds }}" ,
    file_path = "/home/jon/Downloads/Data/Code/code_db_{}.csv",
    bucket_name = 'seattle-test',
    dag = monthly_dag)

download_inventory = DownloadDataOperator(
    task_id="download_inventory",
    db_name ="inventory_db",
    params= {'where':'reportdate > "{{ prev_ds }}" and reportdate <= "{{ ds }}"',\
        'select':"""bibnum,title,isbn,publicationyear,publisher,itemtype,\
            itemcollection,floatingitem,itemlocation,reportdate,itemcount,author,subjects"""},
    date= "{{ ds }}" ,
    file_path = "/home/jon/Downloads/Data/Inventory/",
    dag = monthly_dag)

create_exploded_df = ManipulateInventoryOperator(
    task_id="manipulate_inventory_df",
    dag=monthly_dag,
    date = "{{ ds }}",
    read_file_path = "/home/jon/Downloads/Data/Inventory/inventory_db_{}.csv",
    write_file_path = "/home/jon/Downloads/Data/Subjects/subjects_df_{}.csv")

upload_subjects_to_s3 = UploadDataOperator(
    task_id="upload_subjects_to_s3",
    aws_id = "aws_credentials",
    date= "{{ ds }}" ,
    file_path = "/home/jon/Downloads/Data/Subjects/subjects_df_{}.csv",
    bucket_name = 'seattle-test',
    dag = monthly_dag)

upload_inventory_to_s3 = UploadDataOperator(
    task_id="upload_inventory_to_s3",
    aws_id = "aws_credentials",
    date= "{{ ds }}" ,
    file_path = "/home/jon/Downloads/Data/Inventory/inventory_db_{}.csv",
    bucket_name = 'seattle-test',
    dag = monthly_dag)

stage_inventory_to_redshift = StageToRedshiftOperator(
    task_id="Stage_inventory_to_redshift",
    dag=monthly_dag,
    target_table = "staging_inventory",
    s3_bucket = "seattle-test",
    s3_key = "inventory_db_{{ds}}.csv",
    create_statement = staging_tables.create_inventory_stg)

stage_code_to_redshift = StageToRedshiftOperator(
    task_id="Stage_codes_to_redshift",
    dag=monthly_dag,
    target_table = "staging_codes",
    s3_bucket = "seattle-test",
    s3_key = "code_db_{{ds}}.csv",
    create_statement = staging_tables.create_code_staging)


stage_subjects_to_redshift = StageToRedshiftOperator(
    task_id="Stage_subjects_to_redshift",
    dag=monthly_dag,
    target_table = "staging_exploded_subjects",
    s3_bucket = "seattle-test",
    s3_key = "subjects_df_{{ds}}.csv",
    create_statement = staging_tables.create_exploded_subjects_stg)

load_collection_dimension_table = LoadDataOperator(
    task_id='Load_collection_dim_table',
    dag=monthly_dag,
    table = "dim_collections",
    sql_stmt_create = dim_tables.create_dim_collection,
    sql_stmt_load = dim_tables.insert_dim_collection,
    truncate_existing_table = True)

load_books_dimension_table = LoadDataOperator(
    task_id='Load_books_dim_table',
    dag=monthly_dag,
    table = "dim_books",
    sql_stmt_create = dim_tables.create_dim_books,
    sql_stmt_load = dim_tables.insert_dim_books,
    truncate_existing_table = False)

load_subject_dimension_table = LoadDataOperator(
    task_id='Load_subject_dim_table',
    dag=monthly_dag,
    table = "dim_subject",
    sql_stmt_create = dim_tables.create_dim_subject,
    sql_stmt_load = dim_tables.insert_dim_subject2,
    truncate_existing_table = False)

load_bridge_table = LoadDataOperator(
    task_id='Load_bridging_table',
    dag=monthly_dag,
    table = "bridge_subject",
    sql_stmt_create = bridge_link.create_bridge_subjects,
    sql_stmt_load = bridge_link.insert_bridge_subjects3,
    truncate_existing_table = False)


start_operator >> download_code >> upload_code_to_s3 >> stage_code_to_redshift >> load_collection_dimension_table
start_operator >> download_inventory >> [upload_inventory_to_s3, create_exploded_df] 

upload_inventory_to_s3 >> stage_inventory_to_redshift >> load_books_dimension_table 
create_exploded_df >> upload_subjects_to_s3 >> stage_subjects_to_redshift >> load_subject_dimension_table >>  load_bridge_table

