from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook


class LoadDataOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql_stmt_create,
                 sql_stmt_load,
                 table,
                 truncate_existing_table,
                 *args, **kwargs):

        super(LoadDataOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt_create = sql_stmt_create
        self.sql_stmt_load = sql_stmt_load
        self.table = table
        self.truncate = truncate_existing_table

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("create table if exists {}".format(self.table))
        redshift.run(self.sql_stmt_create)
        if self.truncate:
            redshift.run("truncate table {}".format(self.table)) #not all dims are truncated
        self.log.info("insert into {} dim table".format(self.table))
        redshift.run(self.sql_stmt_load)
        records = redshift.get_records("SELECT count(*) from {}".format(self.table))
        self.log.info(records)
