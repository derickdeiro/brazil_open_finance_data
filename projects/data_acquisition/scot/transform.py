import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from typing import List
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.scot.data_contract import scot_schema
from projects.data_acquisition.scot.constants import (
    estados_brasil,
    cidades_boi_gordo,
    regioes_dict,
    regioes_uf,
    FAMCOML,
    ITF,
    INTERVALO,
    reposicao_dict,
    SOURCE_NAME)
import pandas as pd
import re

class TransformScot(TransformData):
    def __init__(self):
        super().__init__()

    def transform_data(self, blob_path: List[str], exec_date):
        """
        Função que transforma os dados de Scot
        """
        df_concated = pd.DataFrame()

        for blob_name in blob_path:
            raw_data = self.download_raw_data(raw_data_path=blob_name)
            
            file_name = os.path.basename(blob_name)
            asset = file_name.split('.')[0]
            
            if asset in ('boigordo, vacagorda'):
                dataframe = self._get_bovino_data(raw_data, asset)
            elif asset in ('milho, soja'):
                dataframe = self._get_corn_and_soybean_data(raw_data, asset)
            else:
                dataframe = self._get_reposicao_data(raw_data)
            
            df_concated = pd.concat([df_concated, dataframe], axis=0)
                
        df_cleaned = self.clean_data_up(dataframe=df_concated)
                
        df_transformed = self._transform_dataframe_to_default_layout(dataframe=df_cleaned, famcompl=FAMCOML, itf=ITF, intervalo=INTERVALO, original_columns=['valor'], attributes_columns=['a185'])

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=scot_schema)

        output_dict = self.upload_output(output_data=output_data)
        
        return output_dict
    
    def _get_bovino_data(self, content, asset):
        df = pd.DataFrame(content['Cotacao']) 

        df.replace({"Estado": estados_brasil}, inplace=True)
        df['Cidade'] = df['Cidade'].apply(lambda x: x.split('(')[0])
        df['Cidade'] = df['Cidade'].str.strip()
        df.replace({'Cidade': cidades_boi_gordo}, inplace=True)
        df['Cidade'] = df['Cidade'].fillna(df['Estado'])
        df.replace({'Cidade': regioes_dict}, inplace=True)
        df.replace({'Cidade': regioes_uf}, inplace=True)
        df['Cidade'] = df['Cidade'].apply(lambda x: self._remove_non_alphanum(x))

        if asset == 'boigordo':
            df['SERIE'] = 'Boi Gordo - ' + df['Cidade'] + ' - ' + df['Estado']
        else:
            df['SERIE'] = 'Vaca Gorda - ' + df['Cidade'] + ' - ' + df['Estado']
        
        df.rename(columns={'Preco_Livre_Avista': 'valor'}, inplace=True)

        df['DATASER'] = content['Data']
        return df

    def _get_corn_and_soybean_data(self, content, asset):
        df = pd.DataFrame(content['Cotacao'])
        
        df.replace({"Estado": estados_brasil}, inplace=True)
        
        if asset == 'milho':
            df['SERIE'] = 'Milho - ' + df['Cidade'] + '-' + df['Estado']
        else:
            df['SERIE'] = 'Soja - ' + df['Cidade'] + '-' + df['Estado']
        
        df.rename(columns={'Compra': 'valor'}, inplace=True)
        
        df['DATASER'] = content['Data']

        return df


    def _get_reposicao_data(self, content):
        df_concat = pd.DataFrame()
        for breed in content.keys():
            if breed in ('Data', 'Legenda'):
                continue
            for animal in content[breed]['Valores'].keys():
                df_temp = pd.DataFrame(content[breed]['Valores'][animal])
                if 'Fêmea' in breed and animal == 'Desmama':
                    df_temp['SERIE'] = 'Reposição - ' + breed + ' - ' + f'Bza {animal}' + ' - ' + df_temp['UF']
                elif 'Macho' in breed and animal == 'Desmama':
                    df_temp['SERIE'] = 'Reposição - ' + breed + ' - ' + f'Bzo {animal}' + ' - ' + df_temp['UF']
                else:
                    df_temp['SERIE'] = 'Reposição - ' + breed + ' - ' + animal + ' - ' + df_temp['UF']
                    
                df_concat = pd.concat([df_concat, df_temp], axis=0)
                
        df_concat.replace({'SERIE': reposicao_dict}, regex=True, inplace=True)

        df_concat.rename(columns={'R$/cab': 'valor'}, inplace=True)
        
        df_concat['DATASER'] = content['Data']
        
        return df_concat
    
    
    def _remove_non_alphanum(self, text):
        return re.sub(r'[^\w\s]', '', text)