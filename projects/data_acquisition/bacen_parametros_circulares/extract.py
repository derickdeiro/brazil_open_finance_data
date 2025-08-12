import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.bacen_parametros_circulares.constants import (SOURCE_NAME, CODIGOS_SERIES)


class ExtractBACEN(ExtractData):
    def __init__(self):
        super().__init__()

#tirar duvida sobre os parametros
    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo JSON da bacen_parametros_circulares
        """

         #precisa transformar em objeto, ou pode só string e o formato dia mes ano
        initial_date = (exec_date - timedelta(days=5)).strftime('%d/%m/%Y')
        final_date = exec_date.strftime('%d/%m/%Y')

        for codigo_serie in CODIGOS_SERIES:

            url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados?formato=json&dataInicial={initial_date}&dataFinal={final_date}'
            response = self.use_requests(url=url, method='get')

            if response.status_code == 200:
                content = response.content
                self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=f'bacen_{codigo_serie}.json')
                
            else:
                raise f'Erro ao realizar a captura do arquivo Json.'
