import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.anbima_fundos.constants import SOURCE_NAME
import requests
import base64
import json
import os
import math
from airflow.sdk import Variable

class ExtractAnbimaFundos(ExtractData):
    def __init__(self):
        super().__init__()
        
    def extract_data(self, exec_date):
        """
        Função que faz o download da API Anbima Fundos
        """
        resposta = self.genarate_token()
        token = resposta['access_token']

        quantidade_de_paginas = 1
        i = 0
    
        while i <= quantidade_de_paginas:

            anbima_url = f"https://api-sandbox.anbima.com.br/feed/fundos/v2/fundos?page={i}"
        
            header_access = {
            'Content-Type': 'application/json',
            'access_token': token,
            'client_id': Variable.get("client_id_anbima")
            }
            response = self.use_requests(url=anbima_url, method='get', headers=header_access)
            
        
            if response.status_code == 200:
                content = response.json()
                quantidade_de_paginas = math.ceil((content['totalSize']/1000))
                self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=f'dados_anbima_page{i}.json')
                i += 1
                
            else:
                raise f'Erro ao realizar a captura de dados Anbima175.'
            
    @staticmethod
    def genarate_token():
        """
        Função gera Token para acesso a API Anbima Fundos
        """
        anbima_token_url = "https://api.anbima.com.br/oauth/access-token"

        credenciais = f'{Variable.get("client_id_anbima")}:{Variable.get("client_secret_anbima")}'.encode('utf-8')

        credenciais_base = base64.b64encode(credenciais).decode('utf-8')

        header_token = {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {credenciais_base}'
        } 

        requestBody = {
            "grant_type": "client_credentials" 
        }

        dados_json = json.dumps(requestBody) 

        response = requests.post(anbima_token_url, headers=header_token, data=dados_json) 

        try:
            response.raise_for_status()  
            resposta = response.json()
            print(resposta)
            return resposta
        
        except requests.exceptions.RequestException as e:
            print(f"Ocorreu um Erro no acesso: {e}")