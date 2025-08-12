import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from abc import ABC, abstractmethod
from io import BytesIO
from typing import List, TypedDict
import pandas as pd
from airflow.sdk import Variable
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from projects.core.azure_storage import BlobManager
import pandera.pandas as pa
from datetime import date
from workalendar.america import Brazil

ua = UserAgent()


class FileInfo(TypedDict):
    name: str
    file: BytesIO


class ExtractData(BlobManager, ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def extract_data(self, exec_date):
        """Este é um método abstrato que deve ser implementado na classe herdeira para capturar os dados."""
        raise NotImplementedError('Método não implementado.')


    @staticmethod
    def use_requests(url: str, method: str = 'get', vrf: bool = False, **kwargs):
        """
        Método que realiza a requisição HTTP.

        Args:
            url (str): URL a ser requisitada.
            method (str, optional): Método da requisição. Defaults to 'get'.
            **kwargs: Parâmetros da requisição.
        """ 
        header = {'User-Agent': ua.random}
        if 'headers' in kwargs:
            header.update(kwargs['headers'])
        
        if method.lower() == 'get':
            response = requests.get(url, headers=header, verify=vrf, **kwargs)

        elif method.lower() == 'post':
            response = requests.post(url, headers=header, verify=vrf, **kwargs)

        return response


    @staticmethod
    def use_soup(content: requests.Response.content, features: str = 'html.parser', **kwargs) -> BeautifulSoup:
        """
        Método que utiliza o BeautifulSoup para fazer o parse do conteúdo HTML.
        Args:
            content (requests.Response.content): Conteúdo HTML a ser parseado.
            features (str, optional): Parser a ser utilizado. Defaults to 'html.parser'.
            **kwargs: Parâmetros do BeautifulSoup.
        Returns:
            BeautifulSoup: Objeto BeautifulSoup com o conteúdo parseado.
        """ 
    
        soup = BeautifulSoup(content, features=features, **kwargs)
        return soup

    def _verify_holiday(self, date: date) -> bool:
        """
        Método que verifica se a data é um dia útil.

        Args:
            date (dt.date): Data a ser verificada.
        """ 
        verifier = Brazil()
        if verifier.is_working_day(date):
            return True
        else:
            return False


class TransformData(BlobManager, ABC):
    def __init__(self) -> None:
        super().__init__()
        
    @abstractmethod
    def transform_data(self, blob_path: List[str], exec_date) -> str:
        """Este é um método abstrato que deve ser implementado na classe herdeira para realizar as transformações necessárias e criar um DataFrame.

        Returns:
            pd.DataFrame: DataFrame que será utilizado para gerar o Output padrão.
        """       
        
        raise NotImplementedError('Método não implementado.')

    def _transform_dataframe_to_default_layout(
        self,
        dataframe: pd.DataFrame,
        itf: int,
        famcompl: str,
        intervalo: int,
        original_columns: list = None,
        attributes_columns: list = None,
    ) -> pd.DataFrame:
        """Este método é encarregado de transformar o DataFrame recebido no Output padrão da BDS pra ser inserido na base.

        Args:
            dataframe (pd.DataFrame): DataFrame que receberá a transformação.
            itf (int): ITF informado no documento de estruturação de Dados.
            famcompl (str): FAMCOMPL informada no documento de estruturação de Dados.
            intervalo (int): Periodicidade da família. Informada no documento de estruturação de Dados.
            original_columns (list, optional): Lista com os nomes das colunas originais do DataFrame. Defaults to None.
            attributes_columns (list, optional): Lista com os nomes que as colunas do DataFrame devem receber. Defaults to None.

            É de extrema importância que a lista original_columns e attributes_columns sejam do mesmo tamanho e sigam uma ordem de "De/Para".
            Exemplo:
                original_columns = [
                    'O/N',
                    '1W',
                    '2W',
                    'Date',
                    ]

                attributes_columns = [
                    'a589',
                    'a122',
                    'a123',
                    'DATASER',
                ]

        Returns:
            pd.DataFrame: Retorna o DataFrame apenas com as colunas default e renomeadas, seguindo a ordem exigida pelo DataInjestion.
        """

        dataframe['ID'] = itf
        dataframe['FAMCOMPL'] = famcompl
        dataframe['INTERVALO'] = intervalo

        bds_default_columns = [
            'ID',
            'SERIE',
            'DATASER',
            'INTERVALO',
            'FAMCOMPL',
        ]

        if original_columns is not None and attributes_columns is not None:
            rename_columns = dict(zip(original_columns, attributes_columns))

            dataframe.rename(columns=rename_columns, inplace=True)

            filtered_columns = list(
                set(bds_default_columns).union(attributes_columns)
            )
            dataframe = dataframe[filtered_columns]

        dataframe['DATASER'] = pd.to_datetime(
            dataframe['DATASER'], errors='coerce'
        ).dt.strftime('%Y-%m-%d')

        output_dataframe = dataframe[
            bds_default_columns
            + [
                col
                for col in dataframe.columns
                if col not in bds_default_columns
            ]
        ]

        return output_dataframe


    def create_output_dict(self, execution_date, dataframe: pd.DataFrame, identifier: str, schema: pa.DataFrameSchema, cont: int = 1) -> dict:
        """
        Método que cria o dicionário do Output.

        Args:
            execution_date (dt.date): Data de execução do DAG.
            dataframe (pd.DataFrame): DataFrame a ser salvo.
        """         
        execution_date_dt = execution_date.strftime('%Y%m%d')
        output_name = f'out_{identifier}_{cont}_{execution_date_dt}.csv'

        output_file = dataframe.to_json(orient='split')
        output_data = {'file_name': output_name, 'content': output_file, 'schema': schema.to_script()}

        return output_data

    @staticmethod
    def clean_data_up(dataframe: pd.DataFrame) -> pd.DataFrame:
        """     
        Método que realiza a limpeza do DataFrame, removendo valores duplicados, colunas com todos os valores nulos e espaços em branco.

        Args:
            dataframe (pd.DataFrame): DataFrame a ser limpo.
            source_name (str): Nome do source do arquivo.
            dataser (dt.date): Data de execução do DAG.
            file_name (str): Nome do arquivo a ser salvo.
        """         
        df = dataframe.copy(deep=True)
        
        # Remove duplicates values:
        df = df.drop_duplicates()
        
        # Remove column that all values are null
        df = df.dropna(axis=1, how='all')
                        
        return df
        

class LoadData(BlobManager):
    def __init__(self) -> None:
        super().__init__()
   
    @staticmethod
    def call_data_ingest(blob_path: str):
        """Método que chama a função "DataInjest" e realiza o insert do Output no Banco de Dados.

        Args:
            blob_path (str): Caminho em que o Output foi salvo no Blob Storage.

        """
        data_ingest_token = Variable.get('data_ingest_token')
        data_ingest_url = Variable.get('data_ingest_url')
        
        if data_ingest_url[-1] != '/':
            data_ingest_url += '/'
        
        data_ingest = f'{data_ingest_url}api/IngestHttp?file={blob_path}&code={data_ingest_token}'


        response = requests.get(data_ingest)
        print(f'Código de resposta: {response.status_code}')
        if response.status_code != 200:
            raise Exception(f'Falha ao inserir os dados: {response.text}')
        else:
            print(f'Dados inseridos com sucesso. {response.text}')
                
