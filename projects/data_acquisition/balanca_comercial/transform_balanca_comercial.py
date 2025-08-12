import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import TransformData
from projects.data_acquisition.balanca_comercial.data_contract import balanca_comercial_schema
from projects.data_acquisition.balanca_comercial.constants_balanca_comercial import (
    ITF,
    INTERVALO,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
    SOURCE_NAME,
)
import pandas as pd


class TransformBalancaComercial(TransformData):
    def __init__(self) -> None:
        super().__init__()

    def transform_data(self, blob_path, exec_date):
        """
        This method is responsible for transforming the raw data into the default layout.
        
        Args:
            blob_path (str): The path to the raw data.
            exec_date (str): The execution date.
        
        Returns:
            str: The path to the output data.
        """
        pre_dataframe_concat = pd.DataFrame()
        for blob_name in blob_path:
            raw_data = self.download_raw_data(raw_data_path=blob_name)
            df_temp = pd.read_excel(raw_data, sheet_name='DADOS_SH_CGCE')

            df_cleaned = self.clean_data_up(dataframe=df_temp)

            df_pre = self._create_missing_default_columns(dataframe=df_cleaned)

            pre_dataframe_concat = pd.concat([pre_dataframe_concat, df_pre], axis=0)

        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=pre_dataframe_concat,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )
        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=balanca_comercial_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict

    def _create_missing_default_columns(self, dataframe: pd.DataFrame):
        """
        This method is responsible for creating the missing default columns.
        
        Args:
            dataframe (pd.DataFrame): The dataframe to be transformed.
        
        Returns:    
            pd.DataFrame: The dataframe with the missing columns.
            
        """
        building_df = dataframe.copy(deep=True)

        building_df['DATASER'] = building_df.apply(
            lambda row: f"{row['CO_ANO']}"
            + '-'
            + f"{row['CO_MES']}"
            + '-'
            + '01',
            axis=1,
        )

        building_df['SERIE'] = building_df[['NO_CGCE_N3', 'TIPO']].agg(
            ' - '.join, axis=1
        )

        return building_df
