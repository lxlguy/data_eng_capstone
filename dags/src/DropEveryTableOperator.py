from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

class DropEveryTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 *args, **kwargs):

        super(DropEveryTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in ['staging_loans', 'staging_inventory', 'staging_codes',\
            'staging_exploded_subjects','dim_time', 'dim_books','dim_collections',\
            'dim_subject', 'bridge_subject', 'fact_loans']:
            redshift.run("drop table if exists {}".format(table))