import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.bacen_parametros_circulares.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    CODIGOS_SERIES,
    SERIES_NAME,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.bacen_parametros_circulares.data_contract import bacen_parametros_circulares_schema
import pandas as pd
from pendulum import datetime


class TransformBACEN(TransformData):
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
        df_concated = pd.DataFrame()
        for codigo_serie in CODIGOS_SERIES:

            for path in blob_path:
                raw_data = self.download_raw_data(raw_data_path=path)
                df_raw = pd.read_json(raw_data)
                df_temp = pd.DataFrame(df_raw)
                df_temp['SERIE'] = codigo_serie
                df_concated = pd.concat([df_concated, df_temp])

                df = df_concated
                df = self.rename_series_name(df)
                df = self.rename_columns(df)
                df['DATASER'] = pd.to_datetime(df['DATASER'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
        
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=df,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=bacen_parametros_circulares_schema)
        output_dict = self.upload_output(output_data=output_data)

        return output_dict

    @staticmethod
    def rename_series_name(dataframe: pd.DataFrame) -> pd.DataFrame:
        df = dataframe.copy(deep=True)    
        df['SERIE'] = df['SERIE'].map(SERIES_NAME)
        
        return df

    @staticmethod
    def rename_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
        df = dataframe.copy(deep=True)
        
        columns_name = {
                        'data': 'DATASER',
                        'valor': 'a24',
                        }
        
        df.rename(columns=columns_name, inplace=True)
        
        return df
