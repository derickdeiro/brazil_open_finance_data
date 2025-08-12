import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.cvm_funds.constants import SOURCE_NAME
import zipfile
from io import BytesIO

class ExtractCVMFunds(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo Excel da CVM de fundos
        """
        execution_date = exec_date.strftime('%Y-%m-%d')
        
        year = execution_date[:4]
        month = execution_date[5:7]

        URLS = [f'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{year}{month}.zip',
                'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip']

        for url in URLS:
            response = self.use_requests(url)
            if response.status_code == 200:
                zipped_content = zipfile.ZipFile(BytesIO(response.content))
                for content_name in zipped_content.namelist():
                    
                    file_content = zipped_content.read(content_name)

                    file_name = content_name
                    self.upload_raw_data(source_name=SOURCE_NAME, raw_data=file_content, dataser=exec_date, url_or_file_name=file_name)
                
            else:
                raise f'Erro ao realizar a captura do arquivo Excel.'