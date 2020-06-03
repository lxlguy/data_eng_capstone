from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
import os
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    
    """"
    1. create staging table if not exist
    2. truncate staging table if needed
    3. Copies file from s3 storage to staging tables. 
    
    param redshift_conn_id: location of the connection detail in Airflow Connections
    type redshift_conn_id: str

    param target_table: table to be modified
    type target_table: str

    param aws_login: location of the connection detail in Airflow Connections
    type aws_login: str

    param s3_bucket: s3 bucket name to copy from
    type s3_bucket: str

    param s3_key: key of file to be copied
    type s3_key: str

    param create_statement: custom sql to create table, different for each table
    type create_statement:str
    """

    template_fields = ("s3_key",)
       
    copy_table_sql = """
        copy {}
        from 's3://seattle-test/{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        csv delimiter '|' ignoreheader 1
    """

    truncate_table_sql = """
    truncate table {}
    """
    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                target_table="",
                aws_login="",
                s3_bucket="",
                s3_key="",
                create_statement="",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_id = aws_login
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.create_sql_stmt = create_statement

    def execute(self, context):
        aws_hook = AwsHook(self.aws_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        copy_table_stmt = self.copy_table_sql.format(self.target_table, self.s3_key,\
                                                credentials.access_key, credentials.secret_key)       
        redshift.run(self.create_sql_stmt)
        self.log.info('inserting copy_statement:')
        redshift.run(self.truncate_table_sql.format(self.target_table))
        redshift.run(copy_table_stmt)
        records = redshift.get_records("SELECT count(*) from {}".format(self.target_table))
        self.log.info(records[0],' records inserted')            

