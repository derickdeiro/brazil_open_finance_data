import os
import sys


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.cepea_cotacoes_commodities.constants import (SOURCE_NAME, LIST_DICT)

class ExtractCEPEA(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo Excel da DR Universe
        """
        for item in LIST_DICT:
            url = f"https://www.cepea.esalq.usp.br/br/indicador/series/{item['url']}"
            response = self.use_requests(url=url)

            if response.status_code == 200:
                content = response.content

                self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=f'cepea_cotacoes_{item['serie']}.xlsx')
                
            else:
                raise f'Erro ao realizar a captura do arquivo Excel.'

