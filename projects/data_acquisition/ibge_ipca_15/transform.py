import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.ibge_ipca_15.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    SOURCE_NAME,
    MESES, 
    SERIE
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.ibge_ipca_15.data_contract import ipca_15_schema
import pandas as pd
from datetime import datetime


class TransformIPCA15(TransformData):
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
        blob_path = [blob_path]
        df_concat = pd.DataFrame()
        for path in blob_path:
            raw_data = self.download_raw_data(raw_data_path=path)
            df_raw = pd.read_excel(raw_data)
            df = df_raw
            df = df[df.columns[1:]] 
            df = df.dropna()
            df = df.tail(1)
            file_month = df['Unnamed: 1'].values[0]
            df['DATASER'] = self.change_dataser(file_month)
            df['SERIE'] = SERIE
            df = self.rename_attributes(df)
            df_concat = pd.concat([df_concat, df], ignore_index=True)

        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe= df_concat,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=ipca_15_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict

    @staticmethod
    def change_dataser(file_month):
        current_date = datetime.now()
            
        current_month = datetime.strptime(MESES[file_month],'%b')
        if str(current_month.month) == '12':
            file_year = datetime.strftime(current_date.replace(year=current_date.year - 1), '%Y')
            dataser = file_year + '-12-01'
        else:
            temp_date = datetime.strptime(current_date.strftime('%Y-%m'),'%Y-%m')
            dataser = temp_date.strftime('%Y-%m-%d')

        return dataser
    
    @staticmethod
    def rename_attributes(dataframe: pd.DataFrame) -> pd.DataFrame:
        df = dataframe.copy(deep=True)
        
        attrib_columns = {'Unnamed: 2': 'a13',
                    'Unnamed: 3': 'a25'}
        
        df.rename(columns=attrib_columns, inplace=True)
        
        return df
    