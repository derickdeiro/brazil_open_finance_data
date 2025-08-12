import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from typing import List
from projects.data_acquisition.bcb_ptax.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
    CURRENCY_DICT
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.bcb_ptax.data_contract import bcb_ptax_schema
import pandas as pd


class TransformBCBPtax(TransformData):
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

            headers = ['dataCotacao', 'currCode', 'type', 'currency', 'cotacaoCompra', 'cotacaoVenda', 'paridadeCompra', 'paridadeVenda']

            df_raw = pd.read_csv(raw_data, sep=';', encoding='latin1', header=None, names=headers)
    
            df_raw['SERIE'] = df_raw['currency'].map(CURRENCY_DICT)
            
            pre_dataframe_concat = pd.concat([pre_dataframe_concat, df_raw], axis=0)
       
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=pre_dataframe_concat,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )
        df_transformed['c3'] = df_transformed['c3'].apply(self.format_code_currency)
        
        df_transformed = self.format_numeric_columns(df_transformed)
        
        df_transformed = self.convert_date_column_format(df_transformed)

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=bcb_ptax_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict
    
    
    def format_numeric_columns(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        df = dataframe.copy(deep=True)
        numeric_columns = ['a5', 'a30', 'a40', 'a41']

        for column in numeric_columns:
            df[column] = pd.to_numeric(df[column].str.replace(',', '.'), errors='coerce')
            
        return df
    

    def convert_date_column_format(self, dataframe: pd.DataFrame) -> pd.DataFrame: 
        df = dataframe.copy(deep=True)
        
        df['DATASER'] = pd.to_datetime(df['DATASER'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')

        return df
    
    
    def format_code_currency(self, code_value: int) -> str:
        code_value = str(code_value)
        if len(code_value) < 3:
            new_value = '0' * (3 - len(code_value)) + code_value
            return new_value
        else:
            return code_value