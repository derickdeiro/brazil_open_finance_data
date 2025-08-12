import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.cetip_taxa_de_juros.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    SERIE,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
    SOURCE_NAME,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.cetip_taxa_de_juros.data_contract import cetip_schema
import pandas as pd


class TransformCetip(TransformData):
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
                    
            df_raw = pd.DataFrame([raw_data])

            df_cleaned = self.clean_data_up(dataframe=df_raw)

            df_formated = self._transform_json_data(dataframe=df_cleaned)
            
            pre_dataframe = self._add_missing_columns(dataframe=df_formated)

            pre_dataframe_concat = pd.concat([pre_dataframe_concat, pre_dataframe], axis=0)
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=pre_dataframe_concat,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=cetip_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict


    def _transform_json_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        This method is responsible for transforming the raw data into the default layout.
        Args:
            dataframe (pd.DataFrame): The raw data to be transformed.
        Returns:
            pd.DataFrame: The transformed data.
        """
        df_temp = dataframe.copy(deep=True)

        df_temp['rate'] = df_temp['rate'].str.replace(',', '.').astype(float)
        df_temp['date'] = pd.to_datetime(df_temp['date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
                
        return df_temp


    def _add_missing_columns(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        This method is responsible for adding the missing columns to the dataframe.
        Args:
            dataframe (pd.DataFrame): The dataframe to be transformed.
        Returns:
            pd.DataFrame: The transformed dataframe.
        """
        df_temp = dataframe.copy(deep=True)
        
        df_temp['taxa'] = df_temp['rate']
        df_temp['SERIE'] = SERIE
        
        return df_temp

