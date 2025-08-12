import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.fipe_ipc.constants import SOURCE_NAME

class ExtractFipeIPC(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo Excel da DR Universe
        """
        
        year = exec_date.year
        list_dicts = [{'url': f'https://www.fipe.org.br/IndicesConsulta-IPCPesquisa?anos={year}&meses=&categorias=2%2C3%2C4%2C5%2C6%2C7%2C8%2C9&tipo=3', 
                 'name': 'rate'},
                {'url': f'https://www.fipe.org.br/IndicesConsulta-IPCPesquisa?anos={year}&meses=&categorias=2%2C3%2C4%2C5%2C6%2C7%2C8%2C9&tipo=2', 
                 'name': 'index'}]        
        
        for item in list_dicts:
            url = item['url']
            name = item['name']
            response = self.use_requests(url=url, method='get')
            
            if response.status_code == 200:
                content = response.json()
                self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=f'{name}.json')
                
            else:
                raise f'Erro ao realizar a captura do arquivo Json.'
        
