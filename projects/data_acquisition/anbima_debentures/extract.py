import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.anbima_debentures.constants import SOURCE_NAME
from projects.data_acquisition.anbima_debentures.constants import meses_dict
from datetime import timedelta

class ExtractAnbimaDebentures(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo Excel da DR Universe
        """
        
        execution_date = exec_date - timedelta(days=1)
        
        valid_date = self._verify_holiday(date=execution_date)
        
        if valid_date:
        
            execution_date = self.convert_exec_date_to_ref_date(execution_date)
            
            url = f"http://www.anbima.com.br/informacoes/merc-sec-debentures/arqs/D{execution_date}.XLS"
            response = self.use_requests(url=url)

            if response.status_code == 200:
                content = response.content
                self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=f'anbima_debentures_{execution_date}.xls')
            
            else:
                raise f'Erro ao realizar a captura do arquivo xls.'
        else:
            print(f'Não foi possível capturar o arquivo xls. Data {execution_date} é um feriado ou final de semana.')

    @staticmethod
    def convert_exec_date_to_ref_date(date: str) -> str:
        """
        Função que converte a data de execução para a data de referência
        #### Exemplo: 2025-01-01 -> 25jan01
        """
        
        day = date.day
        year = date.strftime('%y')    
        month = date.strftime('%B')
        mes_pt = meses_dict[month]
        execution_date = f'{year}{mes_pt[:3].lower()}{day}'
            
        return execution_date