import os
import sys
import logging
import pandas as pd
import xlrd
from datetime import datetime
from typing import Union 
from io import BytesIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.cepea_cotacoes_commodities.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
    SOURCE_NAME,
    LIST_DICT,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.cepea_cotacoes_commodities.data_contract import cepea_cotacoes_commodities_schema
import pandas as pd
from pendulum import datetime


class TransformCEPEA(TransformData):
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
            raw_data = self.download_raw_data(raw_data_path=path)
            df_raw = create_dataframe(raw_data)
            df = df_raw

            if 'Suino Vivo' in path:
                df = reconstruct_dataframe(df)
                
            df = rename_columns(df)
            df = convert_date(df)
            df = replace_comma_to_dot(df)

            for item in LIST_DICT:
                df['SERIE'] = item['serie']

                if 'Suino Vivo' in path:
                    for i, estado in enumerate(df['ESTADO']):
                        df.loc[i, 'SERIE'] = f'{item['serie']} - {estado}'
                
      
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=df,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=cepea_cotacoes_commodities_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict
    

def create_dataframe(content: Union [BytesIO, bytes]) -> pd.DataFrame:
    """
    Create a DataFrame from the given data.
    """ 
    if isinstance(content, BytesIO):
        content = content.read()
 

    workbook = xlrd.open_workbook(
            file_contents=content,
            ignore_workbook_corruption=True
        )
        
    df = pd.read_excel(workbook, skiprows=3)
    return df.tail(30)


def reconstruct_dataframe(df):
    df = df.melt(id_vars=['Data'], var_name='ESTADO', value_name='À vista R$')
    df = df.sort_values(by=['ESTADO', 'Data']).reset_index(drop=True)
    df = df[['ESTADO', 'Data', 'À vista R$']]
    df['À vista US$'] = ''
    return df 


def rename_columns(df):
 df = df.rename(columns={'Data': 'DATASER', 
                        'À vista R$': 'a24',
                        'À vista US$': 'a12'})
 return df


def convert_date(df):
    df['DATASER'] = pd.to_datetime(df['DATASER'], format='%d/%m/%Y')
    df['DATASER'] = df['DATASER'].dt.strftime('%Y-%m-%d')
    return df


def replace_comma_to_dot(df):
    df['a24'] = df['a24'].replace({',': '.'}, regex=True)
    df['a12'] = df['a12'].replace({',': '.'}, regex=True)
    return df

