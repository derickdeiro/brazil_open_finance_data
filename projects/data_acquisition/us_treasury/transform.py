import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.us_treasury.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.us_treasury.data_contract import us_treasury_schema
import pandas as pd

class TransformUSTreasury(TransformData):
    def __init__(self) -> None:
        super().__init__()
                      
    def transform_data(
        self, blob_path: str, exec_date: str
    ) -> List[pd.DataFrame]:

        df_aux = pd.DataFrame()
        df_output = pd.DataFrame()

        for file in blob_path:
            raw_data = self.download_raw_data(raw_data_path=file)

            df_aux = self.create_df(raw_data, exec_date)       
            df_output = pd.concat([df_output, df_aux], axis=0)

        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=df_output,
            itf=ITF,
            intervalo=INTERVALO,
            famcompl=FAMCOML,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=us_treasury_schema)     
        output_dict = self.upload_output(output_data=output_data)

        return output_dict
    
    @staticmethod
    def create_df(raw_data, data) -> pd.DataFrame:
        df_csv = pd.read_csv(raw_data, sep=',', encoding='utf-8')
        df_csv = df_csv[df_csv['Date'] == data.strftime('%m/%d/%Y')]

        convert_columns ={}

        if 'LT COMPOSITE (>10 Yrs)' in df_csv.columns:
            convert_columns = {
                'LT COMPOSITE (>10 Yrs)': 'US Treasury + 10 anos',
                'TREASURY 20-Yr CMT': 'US Treasury + 25 anos'
            }
        else:
            convert_columns = {
                '1 Mo': 'US Treasury 1 mês',
                '3 Mo': 'US Treasury 3 meses',
                '6 Mo': 'US Treasury 6 meses',
                '1 Yr': 'US Treasury 1 ano',
                '2 Yr': 'US Treasury 2 anos',
                '3 Yr': 'US Treasury 3 anos',
                '5 Yr': 'US Treasury 5 anos',
                '7 Yr': 'US Treasury 7 anos',
                '10 Yr': 'US Treasury 10 anos',
                '20 Yr': 'US Treasury 20 anos',
                '30 Yr': 'US Treasury 30 anos'
            }
        
        df_final = pd.DataFrame()
        df_final.columns = pd.DataFrame(columns=['ID', 'SERIE', 'DATASER', 'INTERVALO', 'a24'])

        for column in convert_columns.keys():
            raw_val = df_csv[column].values[0]

            new_row = {
                'SERIE': convert_columns[column],
                'DATASER': data.strftime('%Y-%m-%d'),
                'a24':  raw_val
            }

            new_row_df = pd.DataFrame([new_row])
            df_final = pd.concat([df_final, new_row_df], ignore_index=True)
            
        return df_final