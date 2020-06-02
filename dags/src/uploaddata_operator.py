from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
import os
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

class UploadDataOperator(BaseOperator):
    template_fields = ("date",)
    
    @apply_defaults
    def __init__(self,
                 aws_id,
                 file_path,
                 date,
                 bucket_name,
                 *args, **kwargs):

        super(UploadDataOperator, self).__init__(*args, **kwargs)
        self.aws_id = aws_id
        self.file_path = file_path
        self.date=date
        self.bucket_name = bucket_name

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_id)
        file_to_upload = self.file_path.format(self.date)        
        file_name_on_s3 = file_to_upload[file_to_upload.rfind('/')+1:]         
        if s3_hook.check_for_key(file_name_on_s3, bucket_name=self.bucket_name):
            _ = s3_hook.delete_objects(bucket = self.bucket_name, keys = file_name_on_s3)
        s3_hook.load_file(file_to_upload, key = file_name_on_s3, bucket_name=self.bucket_name)
        self.log.info('File {} has been uploaded on S3 bucket "{}"'.format(file_name_on_s3, self.bucket_name))
        
