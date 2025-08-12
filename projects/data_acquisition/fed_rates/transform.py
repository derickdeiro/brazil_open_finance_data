import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.fed_rates.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    SOURCE_NAME,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.fed_rates.data_contract import fed_rates_schema
import pandas as pd


class TransformFEDRates(TransformData):
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
        df_concat = pd.DataFrame()
        for path in blob_path:

            raw_data = self.download_raw_data(raw_data_path=path)
                    
            df_raw = pd.read_excel(raw_data, engine='openpyxl')
            
            if 'avg_rates' in path:
                df_raw = self.transform_avg_sofr_data(df=df_raw)
                df_concat = pd.concat([df_concat, df_raw], ignore_index=True)
            elif 'unsecured_rates' in path:
                df_raw['SERIE'] = df_raw['Rate Type'] + ' (Unsecured Overnight Financing Rate)'
                df_raw = self.transform_overnight_data(df=df_raw)
                df_concat = pd.concat([df_concat, df_raw], ignore_index=True)
            elif 'secured_rates' in path:
                df_raw['SERIE'] = df_raw['Rate Type'] + ' (Secured Overnight Financing Rate)'
                df_raw = self.transform_overnight_data(df=df_raw)
                df_concat = pd.concat([df_concat, df_raw], ignore_index=True)
            else: 
                raise ValueError(f"Unknown file type: {path}")
            

        df_cleaned = self.clean_data_up(dataframe=df_concat)

        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=df_cleaned,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=fed_rates_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict

    def transform_avg_sofr_data(self, df: pd.DataFrame) -> pd.DataFrame:
        avg_columns = ['Effective Date', '30-Day Average SOFR', '90-Day Average SOFR', '180-Day Average SOFR', 'SOFR Index']
        
        df = df[avg_columns]
        
        serie_rename = {'30-Day Average SOFR': 'SOFR AVG 30D',
                        '90-Day Average SOFR': 'SOFR AVG 90D',
                        '180-Day Average SOFR': 'SOFR AVG 180D',
                        'SOFR Index': 'SOFR (INDEX)'}
        
        dataframes = []
        for col in df.columns:
            if col != 'Effective Date':
                df_temp = df[['Effective Date', col]]
                dataframes.append(df_temp)
                
        df_concat = pd.DataFrame()
        for df in dataframes:
            df['SERIE'] = df.columns[1]
            df.rename(columns={'Effective Date': 'DATASER', df.columns[1]: 'a23'}, inplace=True)
            df_concat = pd.concat([df_concat, df])
        
        df_concat['DATASER'] = pd.to_datetime(df_concat['DATASER'], errors='coerce').dt.strftime('%Y-%m-%d')
        df_concat['a23'] = df_concat['a23'].astype(str).replace(',', '.').astype(float)
        df_concat['SERIE'] = df_concat['SERIE'].map(serie_rename)
        
        return df_concat
    

    def transform_overnight_data(self, df: pd.DataFrame) -> pd.DataFrame:
    
        renamed_columns = {
                        'Effective Date': 'DATASER',
                        'Rate (%)': 'a23',
                        '1st Percentile (%)': 'a650',
                        '25th Percentile (%)': 'a651',
                        '75th Percentile (%)': 'a652',
                        '99th Percentile (%)': 'a653',
                        'Volume ($Billions)': 'a654'
                        }
        
        df.rename(columns=renamed_columns, inplace=True)
        df['DATASER'] = pd.to_datetime(df['DATASER'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['a23'] = df['a23'].astype(str).replace(',', '.').astype(float)
        df['a654'] = df['a654'].astype(str).replace(',', '').astype(int)
        
        df_filtered = df[['SERIE', 'DATASER', 'a23', 'a650', 'a651', 'a652', 'a653', 'a654']]
                
        return df_filtered
