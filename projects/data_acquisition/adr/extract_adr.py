import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.adr.adr_constants import SOURCE_NAME

class ExtractADR(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo Excel da DR Universe
        """
        adr_url = 'https://api.markitdigital.com/jpmadr-public/v1/drUniverse/exportTable?offset=0&sortBy=name&sortOrder=asc&full=true'
        response = self.use_requests(url=adr_url)

        if response.status_code == 200:
            content = response.content
            self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name='dr_universe.xlsx')
            
        else:
            raise f'Erro ao realizar a captura do arquivo Excel.'
