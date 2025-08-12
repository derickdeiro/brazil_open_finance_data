import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.ibge_ipca_15.constants import SOURCE_NAME
import zipfile
import io

class ExtractIPCA15(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo Excel da DR Universe
        """
        url = 'https://ftp.ibge.gov.br/Precos_Indices_de_Precos_ao_Consumidor/IPCA_15/Series_Historicas/ipca-15_SerieHist.zip'
        response = self.use_requests(url=url, method='get')

    
        if response.status_code == 200:
            file_list = self._extract_arquivo_zip(io.BytesIO(response.content))

            for content in file_list:
                file_content = content['conteudo']
                file_name = content['nome']
                self.upload_raw_data(source_name=SOURCE_NAME, raw_data=file_content, dataser=exec_date, url_or_file_name=f'{file_name}.xlsx')
            
        else:   
            raise f'Erro ao realizar a captura do arquivo Excel.'
        
    def _extract_arquivo_zip(self, file_zip_path):
        
        file_list = []
        with zipfile.ZipFile(file_zip_path, 'r') as file_name:
            for file_zip in file_name.namelist():
                with file_name.open(file_zip) as file:
                    file_list.append({'nome': file_name, 'conteudo': file.read()})
        return file_list

        
