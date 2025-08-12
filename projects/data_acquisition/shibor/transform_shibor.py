import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))       

import pandas as pd
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.shibor.data_contract import shibor_schema
from projects.data_acquisition.shibor.shibor_constants import (
    ATTRIBUTES_COLUMNS,
    FAMCOML,
    INTERVALO,
    ITF,
    ORIGINAL_COLUMNS,
    SOURCE_NAME
)


class TransformShibor(TransformData):
    def __init__(self) -> None:
        super().__init__()

    def transform_data(self, blob_path, exec_date) -> pd.DataFrame:
        """
        This method is responsible for transforming the raw data into the default layout.
        
        Args:
            blob_path (str): The path to the raw data.
            exec_date (str): The execution date.
            
        Returns:
            str: The path to the output data.
        """

        raw_data = []
        for path in blob_path:
            content = self.download_raw_data(raw_data_path=path)
            original_name = os.path.basename(path)
            file_name = original_name.split('.')
            
            shibor_data = {'shibor_file': file_name[0], 'content': content}
            
            raw_data.append(shibor_data)

        df_concat = pd.DataFrame()

        for item in raw_data:
            shibor_file = item['shibor_file']
            content = item['content']
            
            df_temp = pd.read_excel(content, skipfooter=2, engine='openpyxl')    
        
            df_cleaned = self.clean_data_up(dataframe=df_temp)
            
            df_temp = self._create_serie_column(
                dataframe=df_cleaned, shibor_type=shibor_file
            )

            df_output = self._transform_dataframe_to_default_layout(
                dataframe=df_temp,
                itf=ITF,
                famcompl=FAMCOML,
                intervalo=INTERVALO,
                original_columns=ORIGINAL_COLUMNS,
                attributes_columns=ATTRIBUTES_COLUMNS,
            )

            df_concat = pd.concat([df_concat, df_output], ignore_index=True)

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_concat, identifier=ITF, schema=shibor_schema)

        output_dict = self.upload_output(output_data=output_data)

        return output_dict

    def _create_serie_column(
        self, dataframe: pd.DataFrame, shibor_type: str
    ) -> pd.DataFrame:
        """
        This method is responsible for creating the SERIE column.
        
        Args:
            dataframe (pd.DataFrame): The dataframe to be transformed.
            shibor_type (str): The type of shibor data.
            
        Returns:
            pd.DataFrame: The dataframe with the SERIE column.
        """

        df_temp = dataframe.copy(deep=True)

        if shibor_type == 'ShiborHisExcel':
            df_temp['SERIE'] = 'Shibor - Histórico'
        elif shibor_type == 'ShiborMnHisExcel':
            shibor_data = 'Shibor - Mean'
            df_temp['SERIE'] = df_temp['Mean Type'].apply(
                lambda x: f'{shibor_data} - {x}'
            )
        else:
            shibor_data = 'Shibor - Quotes'
            df_temp['SERIE'] = df_temp['Contributor'].apply(
                lambda x: f'{shibor_data} - {x}'
            )

        return df_temp
