from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from azure.storage.blob import BlobClient
import os
from io import BytesIO, StringIO
from typing import Dict, Union
from datetime import datetime as dt
import pandas as pd
import json

BLOB_CONNECTION_STRING = 'bdsrobots_storage'

TODAY = dt.today().strftime('%Y%m%d')

class BlobManager:

    def __init__(self) -> None:
        self.client = WasbHook(wasb_conn_id=BLOB_CONNECTION_STRING)
        self.data_acquisition_container = 'data-acquisition'
        self.import_container = 'import'
        self.blob_service_client = self.client.get_conn()


    def upload_raw_data(
        self, source_name: str, raw_data: any, dataser: str, url_or_file_name: str
    ):
        """
        Método que realiza o upload do arquivo bruto para o Blob Storage.

        Args:
            source_name (str): Nome do source do arquivo.
            raw_data (any): Dados brutos a serem salvos.
            dataser (str): Data de execução do DAG.
            file_name (str): Nome do arquivo a ser salvo.
        """ 
        try:        
            file_name = os.path.basename(url_or_file_name)
        except:
            file_name = url_or_file_name
            
        blob_full_path = self._create_data_acquisition_blob_path(source_name=source_name, dataser=dataser, folder_type='input', file_name=file_name)

        blob_client = self._create_blob_client_path(container=self.data_acquisition_container, blob_path=blob_full_path)

        self._upload_data_into_container(
            blob_client=blob_client, data_content=raw_data
        )
        
        return blob_full_path

    def upload_output(self, output_data: Dict):
        """
        Método que realiza o upload do arquivo transformado para o Blob Storage.

        Args:
            output_data (Dict): Dados a serem salvos.
        """ 

        file_name = output_data['file_name']
        content = output_data['content']
        schema = output_data['schema']

        df = pd.read_json(content, orient='split')

        csv_bytes = self._create_csv_file(dataframe=df)

        blob_full_path = os.path.join(TODAY, file_name).replace(
            os.path.sep, '/'
        )

        blob_client = self._create_blob_client_path(container=self.import_container, blob_path=blob_full_path)

        self._upload_data_into_container(
            blob_client=blob_client, data_content=csv_bytes
        )

        output_dict = {'blob_path': blob_full_path, 'schema': schema}
        
        return output_dict

    @staticmethod
    def _upload_data_into_container(
        blob_client: BlobClient, data_content: Union[BytesIO, bytes, str]
    ):
        """
        Método que realiza o upload do arquivo para o Blob Storage.

        Args:
            blob_client (BlobClient): Cliente do Blob Storage.
            data_content (Union[BytesIO, bytes, str]): Dados a serem salvos.
        """ 
        try:

            if isinstance(data_content, BytesIO):
                data_content.seek(0)
                blob_client.upload_blob(
                    data_content,
                    overwrite=True,
                    metadata={
                        'ignoreBlobTrigger': 'True',
                        'ContentEncoding': 'UTF-8',
                    },
                )

            elif isinstance(data_content, bytes):
                blob_client.upload_blob(
                    data_content,
                    overwrite=True,
                    metadata={
                        'ignoreBlobTrigger': 'True',
                        'ContentEncoding': 'UTF-8',
                    },
                )
            elif isinstance(data_content, (dict, list)):
                data_content = json.dumps(data_content)
                blob_client.upload_blob(
                    data_content,
                    overwrite=True,
                    metadata={
                        'ignoreBlobTrigger': 'True',
                        'ContentEncoding': 'UTF-8',
                    },
                )
            else:

                with open(data_content, 'rb') as data:
                    blob_client.upload_blob(
                        data,
                        overwrite=True,
                        metadata={
                            'ignoreBlobTrigger': 'True',
                            'ContentEncoding': 'UTF-8',
                        },
                    )

            print(f'Conteúdo salvo com sucesso.')
        except Exception as e:
            raise e

    
    def blob_sensor(self, blob_source_name: str, extension: str, exec_date):
        """
        Método que realiza a leitura do Blob Storage para verificar se existe arquivo com a extensão informada.

        Args:
            blob_source_name (str): Nome do source do arquivo.
            extension (str): Extensão do arquivo a ser verificado.
            exec_date (dt.date): Data de execução do DAG.
        """ 

        exec_date_formated = exec_date.strftime('%Y%m%d')
                
        blob_name = f'{blob_source_name}/{exec_date_formated}/input/'
        
        blob_list = self.client.get_blobs_list(container_name=self.data_acquisition_container, prefix=blob_name, delimiter=extension)
      
        if len(blob_list) > 0:
            return blob_list 
        else:
            raise ValueError(f'Falha ao encontrar arquivos com a extensão "{extension}"')


    def _create_data_acquisition_blob_path(self, source_name: str, dataser: dt.date, folder_type: str,  file_name: str):
        """
        Método que cria o caminho do Blob Storage para o arquivo bruto.

        Args:
            source_name (str): Nome do source do arquivo.
            dataser (dt.date): Data de execução do DAG.
            folder_type (str): Tipo de folder a ser criado.
            file_name (str): Nome do arquivo a ser salvo.
        """ 
        execution_date = dataser.strftime('%Y%m%d')

        blob_full_path = os.path.join(
            source_name, execution_date, folder_type , file_name
        ).replace(os.path.sep, '/')
        
        return blob_full_path
    
    
    def _create_blob_client_path(self, container, blob_path):
        """
        Método que cria o cliente do Blob Storage.

        Args:
            container (str): Nome do container.
            blob_path (str): Caminho do arquivo a ser salvo.
        """ 
        blob_client = self.blob_service_client.get_blob_client(
            container=container, blob=blob_path
        )

        return blob_client
            
    
    def download_raw_data(self, raw_data_path: str):
        """
        Método que realiza o download do arquivo bruto do Blob Storage.

        Args:
            raw_data_path (str): Caminho do arquivo a ser baixado.
        """ 
        blob_client = self._create_blob_client_path(container=self.data_acquisition_container, blob_path=raw_data_path)
        
        raw_content = self._download_data(blob_client=blob_client, content_path=raw_data_path)
        
        return raw_content
    
    
    def download_output_data(self, output_path: str):
        """
        Método que realiza o download do arquivo transformado do Blob Storage.

        Args:
            output_path (str): Caminho do arquivo a ser baixado.
        """ 
        blob_client = self._create_blob_client_path(container=self.import_container, blob_path=output_path)
        
        output_data = self._download_data(blob_client=blob_client, content_path=output_path)
        
        return output_data
        
        
    def _download_data(self, blob_client: BlobClient, content_path: str):
        """
        Método que realiza o download do arquivo do Blob Storage.

        Args:
            blob_client (BlobClient): Cliente do Blob Storage.
            content_path (str): Caminho do arquivo a ser baixado.
        """ 
        downloaded_data = blob_client.download_blob()
        
        file_content = downloaded_data.readall()
        
        blob_name = os.path.basename(content_path)
               
        if blob_name.endswith(('.xlsx', '.xls', '.csv', '.parquet')):
            content = BytesIO(file_content)
            content.seek(0)
                   
        elif blob_name.endswith('.json'):
            # Decode the content if it's in bytes
            if isinstance(file_content, bytes):
                file_content = file_content.decode('utf-8')
            
            # Parse the JSON content
            content = json.loads(file_content)
            
            # If the parsed content is a list of dictionaries, convert it back to a JSON string
            if isinstance(content, list) and all(isinstance(i, dict) for i in content):
                content = json.dumps(content)
                
        elif blob_name.endswith('.txt'):
            # Decode the content if it's in bytes
            if isinstance(file_content, bytes):
                try:
                    content = StringIO(file_content.decode('utf-8'))
                except UnicodeDecodeError:
                    content = StringIO(file_content.decode('latin1'))
            else:
                content = StringIO(file_content)  # Assuming it's already a string
        
        elif blob_name.endswith('.xml'):
            if isinstance(file_content, bytes):
                content = file_content.decode('cp1252')
                    
        else:
            raise ValueError(f'Formato não encontrado para leitura: {blob_name}')
        
        return content
    
    def _create_csv_file(self, dataframe: pd.DataFrame):
        """
        Método que cria o arquivo CSV a ser salvo no Blob Storage.
        """
        df_buf = BytesIO()
        dataframe.to_csv(df_buf, index=False, sep='\t', encoding='utf-8')
        df_buf.seek(0)
        csv_bytes = df_buf.getvalue()
        
        return csv_bytes