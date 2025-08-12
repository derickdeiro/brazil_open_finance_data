import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.adr.adr_constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
    SOURCE_NAME,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.adr.data_contract import adr_schema
import pandas as pd
from pendulum import datetime


class TransformADR(TransformData):
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
            
            df_raw = pd.read_excel(raw_data)

            df_cleaned = self.clean_data_up(dataframe=df_raw)

            df_filtered = self._filter_dataframe(raw_data=df_cleaned)

            pre_dataframe = self._create_missing_default_column(
                dataframe=df_filtered, dataser=exec_date
            )
            
            pre_dataframe_concat = pd.concat([pre_dataframe_concat, pre_dataframe], axis=0)
            

        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=pre_dataframe_concat,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=adr_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict

    def _filter_dataframe(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """
        This method is responsible for filtering the raw data.
        
        Args:
            raw_data (pd.DataFrame): The raw data.
            
        Returns:
            pd.DataFrame: The filtered data.
        """
        
        df = raw_data.copy(deep=True)

        brazilian_data = df.loc[df['Country'] == 'Brazil']

        levels = ('LEVEL II', 'LEVEL III')

        brazilian_data['Level'] = brazilian_data['Level'].str.upper()

        filtered_levels = brazilian_data.loc[
            brazilian_data['Level'].isin(levels)
        ]

        interest_columns = ['DR Name', 'Symbol', 'Underlying']

        df_filtered = filtered_levels[interest_columns]

        return df_filtered

    def _create_missing_default_column(
        self, dataframe: pd.DataFrame, dataser: datetime
    ) -> pd.DataFrame:
        """
        This method is responsible for creating the missing default columns.
        
        Args:
            dataframe (pd.DataFrame): The dataframe to be transformed.
            dataser (datetime): The execution date.
        
        Returns:
            pd.DataFrame: The transformed dataframe.
        """

        building_df = dataframe.copy()

        building_df['SERIE'] = (
            building_df[['DR Name', 'Symbol', 'Underlying']]
            .fillna('')
            .agg(' - '.join, axis=1)
        )

        building_df['DATASER'] = dataser
        building_df['Flag'] = 1

        return building_df
