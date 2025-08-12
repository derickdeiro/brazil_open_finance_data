import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.cetip_di.constants import SOURCE_NAME

class ExtractCetipDI(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo Excel da DR Universe
        """
        url = "https://sistemaswebb3-balcao.b3.com.br/featuresDIProxy/DICall/GetIndexDI/eyJsYW5ndWFnZSI6InB0LWJyIn0="
        response = self.use_requests(url=url, method='get')

        if response.status_code == 200:
            content = response.json()
            self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name='cetip_di_data.json')
            
        else:
            raise f'Erro ao realizar a captura do arquivo json.'
