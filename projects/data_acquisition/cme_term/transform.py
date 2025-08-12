import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.cme_term.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
    SOURCE_NAME,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.cme_term.data_contract import cme_term_schema
import pandas as pd


class TransformCMETerm(TransformData):
    def __init__(self) -> None:
        super().__init__()

    def transform_data(
        self, blob_path: str, exec_date: str
    ) -> List[pd.DataFrame]:
        """
        This method is responsible for transforming the raw data into the default layout.
        
        Args:
            blob_path (str): The path to the raw data.
            exec_date (str): The execution date.
        """
        
        pre_dataframe_concat = pd.DataFrame()
        
        for blob_name in blob_path:

            raw_data = self.download_raw_data(raw_data_path=blob_name)

            df_raw = pd.read_json(raw_data)

            df_cleaned = self.clean_data_up(dataframe=df_raw)

            df_cleaned['date'] = pd.to_datetime(df_cleaned['date'], errors='coerce').dt.strftime('%Y-%m-%d')
            
            df_cleaned['rate'] = df_cleaned['rate'].str.replace('%', '').astype(float)

            pre_dataframe = self._clean_pre_series_column(dataframe=df_cleaned)

            pre_dataframe_concat = pd.concat([pre_dataframe_concat, pre_dataframe], axis=0)
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=pre_dataframe_concat,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=cme_term_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict


    def _clean_pre_series_column(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        This method is responsible for cleaning the pre-series column in the dataframe.
        
        Args:
            dataframe (pd.DataFrame): The dataframe to be cleaned.
        
        Returns:
            pd.DataFrame: The cleaned dataframe.
        """
        df = dataframe.copy()
        
        df['term_period'] = df['term_period'].str.replace('CME ', '')
        df['term_period'] = df['term_period'].str.replace(' months', 'M')
        df['term_period'] = df['term_period'].str.replace(' month', 'M')
        
        return df    