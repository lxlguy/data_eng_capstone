from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
import json
from sodapy import Socrata
import pandas as pd
import os

class DownloadDataOperator(BaseOperator):
    template_fields = ("date","params")
    
    @apply_defaults
    def __init__(self,
                 socrata_id,
                 db_name,
                 params,
                 date,
                 file_path,
                 limits=50000,
                 *args, **kwargs):

        super(DownloadDataOperator, self).__init__(*args, **kwargs)
        self.socrata_id = socrata_id
        self.database = db_name
        self.limits = limits
        self.params = params
        self.date = date
        self.file_path = file_path
        

    def execute(self, context):
        connection = BaseHook.get_connection(self.socrata_id)    
        socrates_id, socrates_key, app_token = connection.login, connection.password, connection.schema
        extra = json.loads(connection.extra)    
        client = Socrata("data.seattle.gov", app_token, username=socrates_id,password=socrates_key, timeout=100)
        self.log.info("Established connection to database. Download Limits will be {}".format(self.limits))
        db_string = extra[self.database] 
        if 'where' in self.params:            
            where_cond = self.params['where']                        
        else:
            where_cond = None        
        target_count = client.get(db_string, where=where_cond, select = 'count(*)')
        target_count = int(target_count[0]['count']) #since responses are always encoded as strings
        self.log.info("There are {} records to be downloaded".format(target_count))        
        
        counter = 0
        all_df=[]
        while counter < target_count:
            results = client.get(db_string, limit=self.limits, offset=counter, **self.params)
            temp_df = pd.DataFrame.from_records(results)                        
            if len(df)<1:
                raise Exception ("No data has been downloaded")
            if 'select' in self.params:
                order_cond = self.params['select'].split(',')
                order_cond = [item.lstrip() for item in order_cond] #order the columns in case the response is jumbled up
                temp_df = temp_df[order_cond]                        
            all_df.append(temp_df)
            counter+=self.limits            
        master_df = pd.concat(all_df)
        save_file_path = os.path.join(self.file_path,'{}_{}.csv'.format(self.database, self.date))   
        os.makedirs(save_file_path, exist_ok=True)
        master_df.to_csv(save_file_path, sep='|', index=False)
        self.log.info("{} records saved in {}".format(len(master_df), save_file_path))
