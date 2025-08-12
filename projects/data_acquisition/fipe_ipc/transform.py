import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.fipe_ipc.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    SOURCE_NAME,
    RENAME_SERIE_NAME,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.fipe_ipc.data_contract import ipc_schema
import pandas as pd


class TransformFipeIPC(TransformData):
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
        
        for path in blob_path:
            file_name = os.path.basename(path)
            file_name = file_name.split('.')[0]
            
            raw_data = self.download_raw_data(raw_data_path=path)
                        
            df_raw = self.clean_data(data=raw_data)
            df_raw = self.format_dataser(df=df_raw)
            df_raw = self.create_serie(df=df_raw, ipc_type=file_name)
            
            if file_name == 'rate':
                df_rate = df_raw
            else:
                df_index = df_raw
            
        df_merged = pd.merge(df_index, df_rate, on=['DATASER', 'SERIE'], how='left')
        
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=df_merged,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=ipc_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict


    @staticmethod
    def clean_data(data):
        garbage_columns = ['CodigoTipoModulo', 'CodigoIndiceTipoIPC', 'AnoMes', 'Divulgacao', 'Criacao', 'Ativo', 'Alteracao']

        df = pd.read_json(data, orient='records')
        df = df.drop(columns=garbage_columns)

        return df

    def _fill_month_column(self, value: str) -> str:
        if len(value) <= 1:
            return '0' + str(value)
        else:
            return str(value)


    def format_dataser(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Format the 'DATASER' column in the dataframe.
        Args:
            df (pd.DataFrame): The input dataframe.
        Returns:
            pd.DataFrame: The dataframe with the formatted 'DATASER' column.
        """
        df['Mes'] = df['Mes'].apply(lambda x: self._fill_month_column(str(x)))
        df['DATASER'] = df['Ano'].astype(str) + '-' + df['Mes'] + '-01'
        
        df = df.drop(columns=['Ano', 'Mes'])
        
        return df

    @staticmethod
    def create_serie(df: pd.DataFrame, ipc_type: str) -> pd.DataFrame:
        """
        Create a new dataframe with the series name and the corresponding values.
        Args:
            df (pd.DataFrame): The input dataframe.
            ipc_type (str): The type of IPC ('index' or 'rate').
        Returns:
            pd.DataFrame: The transformed dataframe with the series name and values.
        """
        df_concat = pd.DataFrame()
        for column in df.columns:
            if column != 'DATASER':
                if ipc_type == 'index':
                    df_temp = df[['DATASER', column]].rename(columns={column: 'a25'})
                else:
                    df_temp = df[['DATASER', column]].rename(columns={column: 'a13'})
                    
                df_temp['SERIE'] = f'FIPE - IPC - {column}'
                df_concat = pd.concat([df_concat, df_temp])
                
        df_concat['SERIE'] = df_concat['SERIE'].replace(RENAME_SERIE_NAME, regex=True)
        
        return df_concat