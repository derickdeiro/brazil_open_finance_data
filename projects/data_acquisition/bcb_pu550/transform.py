import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.bcb_pu550.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
    TITLE_LIST,
    SOURCE_NAME,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.bcb_pu550.data_contract import bcb_pu550_schema
import pandas as pd
from pendulum import datetime


class TransformPU550(TransformData):
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
            header = raw_data.readline()
            df_raw = pd.read_fwf(raw_data, skipfooter=1, header=None, names= ['tipo_registro', 'codigo_titulo', 'data_vencimento', 'preco_unitario', 'preco_retorno', 'preco_custodia'], 
                            widths=[1, 6, 8, 18, 18, 13], 
                            parse_dates=['data_vencimento'], decimal=',', thousands='.', encoding='latin1',
                            )
        
            df = self._format_numeric_column(df_raw)
            
            df['descricao'] = df['codigo_titulo'].map(TITLE_LIST)
            
            df['SERIE'] = df['descricao'] + ' ' + df['codigo_titulo'].astype(str) + ' - ' + df['data_vencimento'].astype(str)

            df['DATASER'] = exec_date.strftime('%Y-%m-%d')
        
            pre_dataframe_concat = pd.concat([pre_dataframe_concat, df], axis=0)
        
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=pre_dataframe_concat,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=bcb_pu550_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict

    @staticmethod
    def format_price(price):
        if price == 0:
            return 0
        else:
            return price / 100000000
    
    def _format_numeric_column(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_columns = ['preco_unitario', 'preco_retorno']
        for col in numeric_columns:
            df[col] = df[col].apply(self.format_price) 
        return df

