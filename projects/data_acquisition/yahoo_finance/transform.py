import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.yahoo_finance.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    TICKER_COUNTRY,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.yahoo_finance.data_contract import yahoo_schema
import pandas as pd
from typing import List


class TransformYahooFinance(TransformData):
    def __init__(self) -> None:
        super().__init__()   
        
    def transform_data(self, blob_path: List[str], exec_date: str) -> List[pd.DataFrame]:
        
        df_concat = pd.DataFrame()

        for path in blob_path:
            file_name = os.path.basename(path)
            ticker_symbol = file_name.split('.')[0]

            raw_data = self.download_raw_data(raw_data_path=path)            

            df_raw = pd.read_csv(raw_data, header=[0, 1], index_col=0, parse_dates=True)

            temp_df = self.remove_header(dataframe=df_raw)
            
            pre_transformed_df = self.set_missing_columns(ticker=ticker_symbol, dataframe=temp_df)
            
            df_concat = pd.concat([df_concat, pre_transformed_df], ignore_index=True)
            
            
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=df_concat,
            itf=ITF,
            intervalo=INTERVALO,
            famcompl=FAMCOML,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=yahoo_schema)        
        output_dict = self.upload_output(output_data=output_data)
        
        return output_dict   
    
    
    def remove_header(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Remove header rows from the dataframe.
        """
        dataframe.columns = dataframe.columns.droplevel(1)
        dataframe.columns.name = None
        dataframe.reset_index(inplace=True)
    
        return dataframe
    
    
    def set_missing_columns(self, ticker: str, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma os dados do Yahoo Finance para o formato desejado.
        """
        country = TICKER_COUNTRY[ticker]      
        
        dataframe['Country'] = country
        
        if ticker == 'GSPC':
            ticker = ticker.replace('G', '')
            
        yahoo_code = f"^{ticker}"
        
        dataframe['Ticker'] = yahoo_code
        dataframe['SERIE'] = ticker
        
        return dataframe
