import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.abiove_biodiesel.constants_biodiesel import SOURCE_NAME
from dateutil.relativedelta import relativedelta

class ExtractBiodiesel(ExtractData):
    def __init__(self):
        super().__init__()
    
    def extract_data(self, exec_date):
        """
        Função que faz o download dos arquivos de biodiesel da Abiove
        
        :param exec_date: data de execução da extração
        
        :return: lista com o path dos arquivos brutos
        """
        
        temp_date = exec_date.strftime('%Y%m%d')
        
        day = int(str(temp_date[-2:]))
        
        if day < 10:
            months_to_reduce = 4
        else:
            months_to_reduce = 3
            
        relative_date = exec_date - relativedelta(months=months_to_reduce)
        
        execution_date_dt = relative_date.strftime('%Y-%m-%d')        
        
        year = str(execution_date_dt[:4])
        month = str(execution_date_dt[5:7])
        
        URLS = [f'https://abiove.org.br/abiove_content/Abiove/{year}.{month}-materia_prima.xlsx',
                f'https://abiove.org.br/abiove_content/Abiove/{year}.{month}-producao_entrega.xlsx',
                f'https://abiove.org.br/abiove_content/Abiove/{year}.{month}-venda_importacao_dieselB.xlsx']

        raw_data_list = []
        for url in URLS:                
            try:
                response = self.use_requests(url=url)
                
                if response.status_code == 200:
                    
                    content = response.content
                    
                    raw_data = self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=url)
                    raw_data_list.append(raw_data)
                
            except Exception:
                raise f'Erro ao fazer download do link {url}: {Exception}'
        
        return raw_data_list