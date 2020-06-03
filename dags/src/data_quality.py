from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """"
    Performs basic data quality check using queries
    
    param redshift_conn_id: location of the connection detail in Airflow Connections
    type redshift_conn_id: str

    param queries: list of queries to run
    type queries: list(str)
    
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 queries,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.queries = queries

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        flag_error = False
        for query in self.queries:            
            outcome = redshift.get_records(query)[0]
            if outcome[0]<1:
                flag_error = True
                self.log.warn("For query {}, # of rows in table is <1".format(query))            
        if flag_error:
            raise Exception("At least one data check has failed. Refer to logs")
