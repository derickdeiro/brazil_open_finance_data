import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.fgv_indexes.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,    
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.fgv_indexes.data_contract import (fgv_di_schema, fgv_index_m_schema, fgv_index_10_schema)
import pandas as pd

class TransformFGVIndex(TransformData):
    def __init__(self) -> None:
        super().__init__()

    def transform_data(self, blob_path: str, exec_date: str
    ) -> List[pd.DataFrame]:
       
        """
        This method is responsible for transforming the raw data into the default layout.
        
        Args:
            blob_path (str): The path to the raw data.
            exec_date (str): The execution date.
        """
        
        output_dict_list = []
        
        for index, blob in enumerate(blob_path):
            
            if 'IGP-DI' in blob:
                schema = fgv_di_schema
            elif 'IGP-10' in blob:
                schema = fgv_index_10_schema
            elif 'IGP-M' in blob:
                schema = fgv_index_m_schema

            df_raw = self.download_raw_data(raw_data_path=blob)
            
            df_month = self.read_fgv_index_data(excel_data=df_raw, month_data=True)
            
            df_year = self.read_fgv_index_data(excel_data=df_raw, month_data=False)
            
            df_month_melted = self.melt_fgv_index_data(df=df_month, month_data=True)
            
            df_year_melted = self.melt_fgv_index_data(df=df_year, month_data=False)
            
            df_pre_transformed = self.merge_data(df_month=df_month_melted, df_year=df_year_melted)
            
            df_transformed = self._transform_dataframe_to_default_layout(
                dataframe=df_pre_transformed,
                itf=ITF,
                famcompl=FAMCOML,
                intervalo=INTERVALO,
                original_columns=ORIGINAL_COLUMNS,
                attributes_columns=ATTRIBUTES_COLUMNS,            
            )
            
            df_transformed['SERIE'] = df_transformed['SERIE'].str.strip() + ' - Final'
            
            output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=schema, cont=index)
            
            output_dict = self.upload_output(output_data=output_data)
            
            output_dict_list.append(output_dict)
            
        return output_dict_list
    
    @staticmethod    
    def fix_dataframe_header(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Fix the header of a DataFrame by splitting the first row into multiple columns.
        
        Args:
            dataframe (pd.DataFrame): The DataFrame to fix.
            
        Returns:
            pd.DataFrame: The DataFrame with fixed header.
        """
        dataframe.columns = dataframe.columns.str.split('.').str[0]
        return dataframe


    def read_fgv_index_data(self, excel_data, month_data: bool) -> pd.DataFrame:
        """
        Reads the DI data from an Excel file and returns it as a DataFrame.
        Args:
            excel_data (str): The path to the Excel file containing DI data.
            month_data (bool): If True, reads monthly data; otherwise, reads annual data.
        
        Returns:
            pd.DataFrame: The DataFrame containing the DI data.
        """        
        columns_to_use = 'A:E' if month_data else 'G:K'
        
        df = pd.read_excel(excel_data, skiprows=2, skipfooter=5, usecols=columns_to_use)
            
        df = self.fix_dataframe_header(df)

        if 'Data' not in df.columns:
            df = pd.read_excel(excel_data, skiprows=1, skipfooter=5, usecols=columns_to_use)
            df = self.fix_dataframe_header(df)

        return df
    
    
    def melt_fgv_index_data(self, df: pd.DataFrame, month_data: bool) -> pd.DataFrame:
        """
        Pivots the DI data DataFrame to have 'Data' as index and 'Índice' as columns.
        
        Args:
            df (pd.DataFrame): The DataFrame containing the DI data.
        
        Returns:
            pd.DataFrame: The pivoted DataFrame.
        """
        df = df.tail(3)
        
        value_name_to_use = 'Variação mensal %' if month_data else 'Variação anual %'
        
        df_melted = pd.melt(df, id_vars=['Data'], var_name='Índice', value_name= value_name_to_use)
        
        return df_melted
    
    
    def merge_data(self, df_month: pd.DataFrame, df_year: pd.DataFrame) -> pd.DataFrame:
        """
        Merges monthly and annual data DataFrames.
        
        Args:
            df_month (pd.DataFrame): The DataFrame containing monthly data.
            df_year (pd.DataFrame): The DataFrame containing annual data.
        
        Returns:
            pd.DataFrame: The merged DataFrame.
        """
        df_merged = pd.merge(df_month, df_year, on=['Data', 'Índice'], how='outer')
        
        return df_merged