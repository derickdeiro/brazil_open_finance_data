import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))       

from projects.core.data_acquisition import TransformData
from projects.data_acquisition.bndes_moedas_contratuais.data_contract import moedas_contratuais_schema
import pandas as pd
from projects.data_acquisition.bndes_moedas_contratuais.constants import (
    SERIES_360_LINHAS,
    SERIES_TRIMESTRAIS,
    SOURCE_NAME,
    ITF,
    FAMCOML,
    ORIGINAL_COLUMNS,
    ATRIBUTES_COLUMNS)
import requests
from io import StringIO

class TransformMoedasContratuais(TransformData):
    def __init__(self):
        super().__init__()
        self.currency_code_list = self.get_currency_code_list()
        
    def transform_data(self, blob_path, exec_date):
        """
        This method is responsible for transforming the raw data into the default layout.
        
        Args:
            blob_path (str): The path to the raw data.
            exec_date (str): The execution
            
        Returns:
            str: The path to the output data.
        """
        
        df_concat = pd.DataFrame()
        
        for path in blob_path:
            content = self.download_raw_data(raw_data_path=path)
            original_name = os.path.basename(path)
            file_name = original_name[:-4]
            
            df_temp = pd.read_csv(StringIO(content), sep=';')
            df_temp.rename(columns={f'{df_temp.columns[0]}': 'data', f'{df_temp.columns[1]}': 'valor'}, inplace=True)
            df_temp = df_temp.replace(r'\t', '', regex=True)
            df_temp = self.remove_comma(df=df_temp)
            df_temp['currency_symbol'] = file_name
            df_temp['currency_code'] = self.get_currency_code(serie_name=file_name)
            
            df_concat = pd.concat([df_concat, df_temp], axis=0)
    
        df_cleaned = self.clean_data_up(dataframe=df_concat)
        
        df_pre_transformed = self.get_recent_date(dataframe=df_cleaned)
        df_pre_transformed['SERIE'] = df_pre_transformed['currency_symbol']
        
        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=df_pre_transformed,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=None,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATRIBUTES_COLUMNS
        )
        
        df_transformed['INTERVALO'] = df_transformed['SERIE'].apply(lambda x: self.get_interval(serie_name=x))
        df_transformed['a24'] = df_transformed['a24'].str.replace(',', '.')
        df_transformed['a24'] = pd.to_numeric(df_transformed['a24'], errors='coerce')
        
        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=moedas_contratuais_schema)
        
        output_dict = self.upload_output(output_data=output_data)
        
        return output_dict
    
    def remove_comma(self, df):
        """
        This method is responsible for removing commas from the DataFrame.
        
        Args:
            df (pd.DataFrame): The DataFrame to be transformed.
            
        Returns:
            pd.DataFrame: The DataFrame with commas removed.
        """
        
        for column in df.columns:
            df[column] = df[column].apply(lambda x: str(x).replace(',', '.'))
        
        return df
    
    def get_recent_date(self, dataframe: pd.DataFrame):
        """
        This method is responsible for getting the most recent date from the DataFrame.
        
        Args:
            dataframe (pd.DataFrame): The DataFrame to be transformed.
            
        Returns:
            pd.DataFrame: The DataFrame with the most recent date.
        """
        
        df = dataframe.copy(deep=True)

        df[df.columns[0]] = pd.to_datetime(df[df.columns[0]], format='%d/%m/%Y', errors='coerce')
        df = df.loc[df[df.columns[0]] == df[df.columns[0]].max()]

        return df
    
    def get_interval(self, serie_name: str):
        """
        This method is responsible for getting the interval of the series.
        
        Args:
            serie_name (str): The name of the series.
            
        Returns:
            int: The interval of the series.
        """

        if serie_name in SERIES_360_LINHAS:
            return 9
        elif serie_name in SERIES_TRIMESTRAIS:
            return 6
        else:
            return 1
        
    def get_currency_code_list(self):
        """
        This method is responsible for getting the currency code list.
        
        Returns:
            pd.DataFrame: The currency code list.
        """
        lista = requests.get('https://www.bndes.gov.br/SiteBNDES/bndes/bndes_pt/Galerias/Convivencia/Moedas_Contratuais/lista_moedas.html')
        df_list = pd.read_html(lista.text)
        currency_code_list = pd.DataFrame(df_list[0], columns=['Sigla', 'Código'])
        
        return currency_code_list
    
    def get_currency_code(self, serie_name: str):
        """
        This method is responsible for getting the currency code.
        
        Args:
            serie_name (str): The name of the series.
            
        Returns:
            str: The currency code.
        """
        try:
            currency_code = self.currency_code_list.loc[self.currency_code_list['Sigla'] == serie_name, 'Código'][0]
        except:
            currency_code = serie_name
        return currency_code