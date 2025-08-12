import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.bcb_pu550.constants import SOURCE_NAME

class ExtractPU550(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo txt da bcb_pu550.
        """
        execution_date = exec_date.strftime('%Y-%m-%d')
        url = f'https://www3.bcb.gov.br/novoselic/rest/arquivosDiarios/pub/download/2/{execution_date.replace('-', '')}ASEL006'
        response = self.use_requests(url=url, method='get')

        if response.status_code == 200:
            content = response.content
           
            self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name='bcb_ASEL006.txt')
            
        else:
            raise f'Erro ao realizar a captura do arquivo txt.'
