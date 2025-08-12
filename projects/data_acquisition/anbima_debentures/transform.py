import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.anbima_debentures.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.anbima_debentures.data_contract import anbima_debentures_schema
import pandas as pd


class TransformAnbimaDebentures(TransformData):
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
                    
            df_raw = pd.ExcelFile(raw_data, engine='xlrd')

            df_concated = self._concat_sheets(dataframe=df_raw)
            
            df_formated = self._format_dtypes(dataframe=df_concated)
            
            df_pre_output = self._set_missing_default_columns(dataframe=df_formated, execution_date=exec_date)
        
            pre_dataframe_concat = pd.concat([pre_dataframe_concat, df_pre_output], axis=0)
        
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=pre_dataframe_concat,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=anbima_debentures_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict


    @staticmethod
    def _concat_sheets(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        This method is responsible for concatenating all sheets into a single DataFrame.
        
        Args:
            dataframe (pd.DataFrame): The DataFrame to be concatenated.
        
        Returns:
            pd.DataFrame: The concatenated DataFrame.
        """
        df_concat = pd.DataFrame()
        for sheet_name in dataframe.sheet_names:
            df_temp = pd.read_excel(dataframe, sheet_name=sheet_name, skiprows=7, skipfooter=5)
            
            df_temp.rename(columns={'Intervalo Indicativo': 'Mínimo', 'Unnamed: 9': 'Máximo'}, inplace=True)
            
            df_concat = pd.concat([df_concat, df_temp[1:]], ignore_index=True)
            
        return df_concat


    @staticmethod
    def _set_missing_default_columns(dataframe: pd.DataFrame, execution_date) -> pd.DataFrame:
        """
        This method is responsible for setting the missing default columns in the DataFrame.
        
        Args:
            dataframe (pd.DataFrame): The DataFrame to be set.
        
        Returns:
            pd.DataFrame: The DataFrame with the missing default columns set.
        """
        dataframe['SERIE'] = dataframe['Código']
        dataframe['DATASER'] = execution_date
                
        return dataframe
    
    
    @staticmethod
    def _format_dtypes(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        This method is responsible for formatting the date in the DataFrame.
        
        Args:
            dataframe (pd.DataFrame): The DataFrame to be formatted.
        
        Returns:
            pd.DataFrame: The formatted DataFrame.
        """
        date_columns = ['Repac./  Venc.', 'Referência NTN-B']
        
        float_columns = ['Taxa de Compra','Taxa de Venda', 'Taxa Indicativa', 'Desvio Padrão', 'Mínimo', 'Máximo', 'PU', '% Pu Par', 'Duration']
        
        for col in dataframe.columns:
            if col in date_columns:
                dataframe[col] = pd.to_datetime(dataframe[col], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
            
            if col in float_columns:
                dataframe[col] = dataframe[col].str.replace('--', '0')
                dataframe[col] = dataframe[col].str.replace('N/D', '0')
                dataframe[col] = pd.to_numeric(dataframe[col], errors='coerce')
                dataframe[col] = dataframe[col].astype(float)
        
        return dataframe