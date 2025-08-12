import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.anbima_indices_ida.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.anbima_indices_ida.data_contract import anbima_indices_ida_schema
import pandas as pd


class TransformAnbimaIDA(TransformData):
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
        
            pre_dataframe_concat = pd.concat([pre_dataframe_concat, df_pre_output], axis=0)
        
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=pre_dataframe_concat,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=anbima_indices_ida_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict


    @staticmethod
    def _rename_series(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Renomeia a coluna SERIE para manter a consistência com o padrão esperado.
        """
        dataframe['indice'] = dataframe['indice'].str.replace('-', ' - ', regex=True)
        
        series_name = {
                    'IPCA_EX_INFRAESTRUTURA': 'IPCA ex-Infraestrutura',
                    'IPCA_INFRAESTRUTURA': 'IPCA Infraestrutura',
                    }

        dataframe['indice'] = dataframe['indice'].replace(series_name, regex=True)

        return dataframe