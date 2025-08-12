import json
import os
import sys
import xlrd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.cepea_ind_agropecuarios.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.cepea_ind_agropecuarios.data_contract import cepea_schema
import pandas as pd
from typing import List


class TransformCepeaIndAgropecuarios(TransformData):
    def __init__(self) -> None:
        super().__init__()   
        
    def transform_data(self, blob_path: List[str], exec_date: str) -> List[pd.DataFrame]:
        output_list = []

        for count, path in enumerate(blob_path):
            original_name = os.path.basename(path)
            file_name = original_name.split('.')
            file_name = file_name[0].replace("_", "/")

            raw_data = self.download_raw_data(raw_data_path=path)            

            df_raw = self.create_dataframe(raw_data)
            
            df_raw["Data"] = pd.to_datetime(df_raw["Data"], format="%d/%m/%Y", dayfirst=True)
            
            df_pre_tranformed = df_raw.tail(30)
            
            df_pre_tranformed['SERIE'] = file_name
            
            df_transformed = self._transform_dataframe_to_default_layout(
                dataframe=df_pre_tranformed,
                itf=ITF,
                intervalo=INTERVALO,
                famcompl=FAMCOML,
                original_columns=ORIGINAL_COLUMNS,
                attributes_columns=ATTRIBUTES_COLUMNS,
            )

            output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=cepea_schema, cont=count)        
            output_dict = self.upload_output(output_data=output_data)
            
            output_list.append(output_dict)
            
            count += 1
        
        return output_list                           
    
    @staticmethod
    def create_dataframe(content):
        """
        Create a DataFrame from the given data.
        """    
        if hasattr(content, "read"):
            content = content.read()

        workbook = xlrd.open_workbook(
                file_contents=content,
                ignore_workbook_corruption=True
            )
        
        df = pd.read_excel(workbook, skiprows=3)
        return df
    
