import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.anbima_indices_ida.constants import SOURCE_NAME
from anbima_utils.access_anbima_api_data import get_anbima_data
from datetime import timedelta

class ExtractAnbimaIDA(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo Excel da DR Universe
        """
        
        url = 'https://api.anbima.com.br/feed/precos-indices/v1/indices/resultados-ida-fechado'
        
        response = get_anbima_data(url_data=url)
        
        if response.status_code == 200:
            content = response.json()
            self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=f'{url}.json')
        
        else:
            raise f'Erro ao realizar a captura do arquivo JSON.'

