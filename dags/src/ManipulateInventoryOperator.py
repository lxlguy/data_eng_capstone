from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
import os
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
import pandas as pd

class ManipulateInventoryOperator(BaseOperator):
    template_fields = ("date",)

    def tidy_split(self,df, column, new_column, sep=',', keep=False):
        """
        Split the values of a column and expand so the new DataFrame has one split
        value per row. Filters rows where the column is missing.
        Modified from https://stackoverflow.com/questions/12680754/split-explode-pandas-dataframe-string-entry-to-separate-rows
        Params
        ------
        df         : pandas.DataFrame
                dataframe with the column to split and expand
        column     : str
                the column to split and expand
        new_column : str
                    name of the new column containing the split values
        sep        : str
                the string used to split the column's values
        keep       : bool
                whether to retain the presplit value as it's own row

        Returns
        -------
        pandas.DataFrame
            Returns a dataframe with the same columns as `df`.
        """
        indexes = list()
        new_values = list()
        df = df.dropna(subset=[column])
        for i, presplit in enumerate(df[column].astype(str)):
            values = presplit.split(sep)
            if keep and len(values) > 1:
                indexes.append(i)
                new_values.append(presplit)
            for value in values:
                indexes.append(i)
                new_values.append(value)
        new_df = df.iloc[indexes, :].copy()
        new_df[new_column] = new_values
        return new_df

    @apply_defaults
    def __init__(self,                                  
                 date,
                 read_file_path,
                 write_file_path,
                 *args, **kwargs):

        super(ManipulateInventoryOperator, self).__init__(*args, **kwargs)        
        self.date = date
        self.read_file_path = read_file_path
        self.write_file_path = write_file_path

    def execute(self, context):
        curr_file = self.read_file_path.format(self.date)
        new_path = self.write_file_path.format(self.date)
        self.log.info("I am looking for file at {}".format(curr_file))
        if os.path.exists(curr_file):
            df = pd.read_csv(curr_file, sep = '|', index_col=None)
        else:
            raise Exception ("Inventory df not found")
        subjects = df['subjects'].unique()
        subjects_df = pd.DataFrame({'subjects':subjects})
        subjects_df = self.tidy_split(subjects_df, 'subjects', 'subject', sep=',', keep=False)
        subjects_df.to_csv(new_path, sep='|', index=False)        
        self.log.info("{} records saved in {}".format(len(subjects_df), new_path))

