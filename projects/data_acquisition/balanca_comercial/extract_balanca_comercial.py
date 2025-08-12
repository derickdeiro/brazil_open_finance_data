import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.balanca_comercial.constants_balanca_comercial import SOURCE_NAME
import os

class ExtractBalancaComercial(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        This method is responsible for extracting the raw data from the source.
        """
        url = 'https://balanca.economia.gov.br/balanca/SH/CGCE.xlsx'
        response = self.use_requests(url=url)
        if response.status_code == 200:
            content = response.content
            self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=url)
                    
        else:
            raise f'Erro ao realizar a captura do arquivo Excel'
