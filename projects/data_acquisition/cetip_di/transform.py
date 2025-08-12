import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.cetip_di.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    SERIE,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.cetip_di.data_contract import cetip_di_schema
import pandas as pd


class TransformCetipDI(TransformData):
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

            df_formated = self._transform_json_data(dataframe=df_raw)
            
            df_formated['SERIE'] = SERIE
            
            pre_dataframe_concat = pd.concat([pre_dataframe_concat, df_formated], axis=0)
        
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=pre_dataframe_concat,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=cetip_di_schema)
        
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

        df_temp['index'] = df_temp['index'].str.replace('.', '').str.replace(',', '.').astype(float)
        df_temp['date'] = pd.to_datetime(df_temp['date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
                
        return df_temp

