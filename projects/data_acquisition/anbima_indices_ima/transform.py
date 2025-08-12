import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.anbima_indices_ima.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.anbima_indices_ima.data_contract import anbima_indices_ima_schema
import pandas as pd


class TransformAnbimaIMA(TransformData):
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

            df_pre_output = self._rename_series(dataframe=df_raw)

            df_pre_output = self._remove_useless_columns(dataframe=df_pre_output)
        
            pre_dataframe_concat = pd.concat([pre_dataframe_concat, df_pre_output], axis=0)
        
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=pre_dataframe_concat,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=anbima_indices_ima_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict


    @staticmethod
    def _rename_series(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Renomeia a coluna SERIE para manter a consistência com o padrão esperado.
        """
        for indice in dataframe['indice'].unique():
            if indice =='IMA-B':
                dataframe.loc[dataframe['indice'] == indice, 'indice'] = 'IMA-B Total'
            elif indice =='IMA-C':
                dataframe.loc[dataframe['indice'] == indice, 'indice'] = 'IMA-C Total'
            elif indice =='IMA-GERAL':
                dataframe.loc[dataframe['indice'] == indice, 'indice'] = 'IMA-Geral Total'
            elif indice =='IMA-GERAL-EX-C':
                dataframe.loc[dataframe['indice'] == indice, 'indice'] = 'IMA-Geral ex-C'
            elif indice =='IMA-S':
                dataframe.loc[dataframe['indice'] == indice, 'indice'] = 'IMA-S Total'
            elif indice =='IRF-M':
                dataframe.loc[dataframe['indice'] == indice, 'indice'] = 'IRF-M Total'

        return dataframe
    
    @staticmethod
    def _remove_useless_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Remove colunas que não são necessárias para o processamento.
        """
        df = dataframe.copy(deep=True)

        df.drop(columns=['quantidade_titulos'], inplace=True, errors='ignore')

        return df