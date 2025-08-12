import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from typing import List
from projects.data_acquisition.ecb_taxa_cambio_euro.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
    SOURCE_NAME,
    CURRENCY_DICT
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.ecb_taxa_cambio_euro.data_contract import ecb_taxa_cambio_euro_schema
import pandas as pd

class TransformECBTaxaCambioEuro(TransformData):
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

            df_raw = pd.read_csv(raw_data, sep=',')
            print(df_raw.head())

            df_formated = self.transform_eurofx_date(dataframe=df_raw)
            
            df_cleaned = self.clean_data_up(dataframe=df_formated)

            pre_dataframe_concat = pd.concat([pre_dataframe_concat, df_cleaned], axis=0)

        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=pre_dataframe_concat,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )

        df_transformed['SERIE'] = df_transformed['SERIE'].map(CURRENCY_DICT)
        
        df_transformed['SERIE'].dropna(inplace=True, axis=0)

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=ecb_taxa_cambio_euro_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict
    
    def transform_eurofx_date(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        df = dataframe.copy(deep=True)
        
        df['Date'] = pd.to_datetime(df['Date'], format='%d %B %Y').dt.strftime('%Y-%m-%d')

        df_melted = df.melt(id_vars='Date', var_name='Ticker', value_name='Value')
        
        df_melted['Ticker'] = df_melted['Ticker'].str.strip()
        
        df_melted['SERIE'] = df_melted['Ticker']
        
        if isinstance(df_melted.iloc[-1]['Value'], str):
            df_melted = df_melted[:-1]
        
        return df_melted