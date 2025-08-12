import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

import base64
from projects.core.data_acquisition import ExtractData
from airflow.sdk import Variable
from projects.data_acquisition.scot.constants import SOURCE_NAME, ASSETS

class ExtractScot(ExtractData):
    def __init__(self):
        super().__init__()
    
    def extract_data(self, exec_date):
        """
        Função que faz o download dos arquivos de Scot
        """
        
        raw_data_list = []
        for asset in ASSETS:
            content = self._get_scot_content(asset)
            
            raw_data = self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=f'{asset}.json')
            raw_data_list.append(raw_data)
            
        return raw_data_list
    
        
    def _get_scot_content(self, asset):
        username = Variable.get('username_scot')
        password = Variable.get('password_scot')
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

        headers = {
            "Authorization": f"Basic {encoded_credentials}"
        }
        # http://apiscotconsultoria.servicos.ws/json/BDS/doc.php
        URL = 'http://apiscotconsultoria.servicos.ws/json/BDS/?cotacao='

        url = f'{URL}{asset}'

        response = self.use_requests(url, headers=headers)

        content = response.json()

        return content