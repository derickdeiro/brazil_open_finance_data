import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

import os
from pendulum import datetime, duration
from airflow.sdk import task # alterado de airflow.decorators para airflow.sdk
from projects.core.schema_validator import SchemaValidator
from projects.core.azure_storage import BlobManager
from projects.core.data_acquisition import LoadData
from core.data_acquisition import ExtractData
from core.data_acquisition import TransformData
from typing import Type, Collection


class PipelineConstructor:
    def __init__(self) -> None:
        pass

    def set_dag_settings(self,
                        dag_name: str,
                        dag_description: str,
                        start_date: str,
                        schedule_interval: str,
                        source_folder_name: str,
                        tags: Collection[str] = None,
                        recursive: bool = False,
                        owner: str = 'DBS_Dados',
                        retries_qtd: int = 3,
                        retries_delay_minutes: int = 1,
                        ) -> dict:
        """Método utilizado para definir todas as variáveis necessárias para criação da DAG.

        Args:
            dag_name (str): Título da DAG que será exibido no Airflow
            dag_description (str): Descrição da ETL
            start_date (str): Data de início no formato: "YYYY-MM-DD"
            schedule_interval (str): Periodicidade da captura no formato cron (https://crontab.guru/#0_8_*_*_1-5): "@daily", "@monthly", "0 18 * * 1-5".
            recursive (bool): Informa se é para rodar a DAG que não rodaram em dias anteriores desde a data de início.
            owner (str): Departamento responsável pela DAG
            retries_qtd (int): Quantidade de tentativas que rodarão em caso de falha.
            retries_delay_minutes (int): Minutos de espera entre as tentativas.
            source_folder_name (str): Nome da pasta correspondente a criação da DAG.

        Returns:
            dict: Retorna um dicionário que deverá ser desempacotado na criação da DAG.
        """
        year = int(start_date[:4])
        month = int(start_date[5:7])
        day = int(start_date[-2:])
            
        default_args = self._set_dag_retries_options(owner=owner, retries_qtd=retries_qtd, retries_delay_minutes=retries_delay_minutes)
        
        # documentation = self._read_requisition(file_folder=source_folder_name)  
        
        settings = {
                'dag_id': dag_name,
                'description': dag_description,
                'start_date': datetime(year=year, month=month, day=day, tz='America/Sao_Paulo'),
                'schedule': schedule_interval,
                'catchup': recursive,
                'tags': tags,
                'default_args': default_args,
                # 'doc_md': documentation,
        }
        
        return settings
    
    @staticmethod
    def _set_dag_retries_options(owner: str, retries_qtd: int, retries_delay_minutes: int) -> dict:
        """Define quais são os critérios para rodar novamente a DAG em caso de falha.

        Args:
            owner (str): Departamento responsável pela DAG
            retries_qtd (int): Quantidade de tentativas que rodarão em caso de falha.
            retries_delay_minutes (int): Minutos de espera entre as tentativas.

        Returns:
            dict: Dicionário contendo as informações definidas para serem adicionadas na DAG.
        """
        default_args={
            'owner': owner,
            'retries': retries_qtd,
            'retry_delay': duration(minutes=retries_delay_minutes),
            'retry_exponential_backoff': True,
            'max_retry_delay': duration(hours=2),
        }
        return default_args

    @staticmethod
    def _read_requisition(file_folder: str):
        """Realiza a leitura do Markdown contendo as informações da Estruturação.

        Args:
            file_folder (str): Nome da pasta que será lida o arquivo "requisition.md"

        Returns:
            file: Requisition lido.
        """
        file_path = os.path.join(f'projects', 'data_acquisition', file_folder, 'requisition.md')
        
        with open(file_path, 'r') as file:
            dag_docs_md = file.read()

        return dag_docs_md

    @staticmethod
    def etl_tasks(extract_class: Type[ExtractData],
                blob_source_name: Type[str],  
                transform_class: Type[TransformData], 
                file_extension: Type[str],
                ):
        """Método utilizado para criar as tarefas da DAG.
        
        Args:
            extract_class (class): Função que realiza a extração dos dados.
            blob_source_name (str): Nome do Blob que será verificado.
            transform_class (class): Função que realiza a transformação dos dados.
            data_schema (dict): Esquema de validação dos dados.
            file_extension (str): Extensão do arquivo que será verificado.
            
        """

        @task(task_id='Extrair_Dados')
        def task_extract(logical_date):
            """Tarefa que realiza a extração dos dados."""
            extract = extract_class()
            return extract.extract_data(exec_date=logical_date)

        @task(task_id='Verificar_Dados_Brutos')
        def task_check_files_in_blob(logical_date):
            """Tarefa que verifica se os arquivos estão disponíveis no Blob."""
            sensor = BlobManager()
            return sensor.blob_sensor(blob_source_name=blob_source_name, extension=file_extension, exec_date=logical_date)

        @task(task_id='Transformar_Dados')
        def task_transform(blob_path, logical_date):
            """Tarefa que realiza a transformação dos dados."""
            transform = transform_class()
            return transform.transform_data(blob_path=blob_path, exec_date=logical_date)

        @task(task_id='Validar_Output')
        def task_validate_schema(output_list):
            """Tarefa que valida o esquema dos dados."""
            validate_schema = SchemaValidator()
            
            if type(output_list) is dict:
                output_list = [output_list]
            
            for output_dict in output_list:
                if 'blob_path' not in output_dict or 'schema' not in output_dict:
                    raise ValueError("O dicionário de saída não contém as chaves 'blob_path' e 'schema'.")
                
                blob_path = output_dict['blob_path']
                data_schema = output_dict['schema']
                
                local_vars = {}
                exec(data_schema, {}, local_vars)

                schema = local_vars["schema"]
                validate_schema.validate_output_schema(schema=schema, output_path=blob_path)

        @task(task_id='Carregar_Dados_no_DataBase')
        def task_insert_data_into_database(output_list):
            """Tarefa que realiza a inserção dos dados no banco de dados."""
            load = LoadData()
            
            if type(output_list) is dict:
                output_list = [output_list]
                
            for output_dict in output_list:
                if 'blob_path' not in output_dict:
                    raise ValueError("O dicionário de saída não contém a chave 'blob_path'.")
                blob_path = output_dict['blob_path']
                load.call_data_ingest(blob_path=blob_path)

    # ---------------------------------------------------------------------------            
        # Declarar dependências
        extract = task_extract()
        sensor = task_check_files_in_blob()
        transform = task_transform(sensor)
        validate_schema = task_validate_schema(transform)
        insert_data = task_insert_data_into_database(transform)
        

        extract >> sensor
        sensor >> transform
        transform >> validate_schema
        validate_schema >> insert_data

                