from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

class DropTableOperator(BaseOperator):
    """"
    Loads the data from staging tables into dim, fact or bridge table

    param redshift_conn_id: redshift connection detail stored in Airflow Connections
    type redshift_conn_id: str

    param table: table to drop in redshift
    type table: str

    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 *args, **kwargs):

        super(DropTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run("drop table if exists {}".format(self.table))
