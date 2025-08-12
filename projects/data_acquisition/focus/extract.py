import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.focus.constants import SOURCE_NAME

class ExtractFocus(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        
        initial_date = (exec_date - timedelta(days=5)).strftime('%Y-%m-%d')
        final_date = exec_date.strftime('%Y-%m-%d')
        
        urlMensal = url = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativaMercadoMensais?$top=10000&$filter=(Indicador eq 'IPCA') and Data ge '{initial_date}' and Data le '{final_date}'&$format=text/csv&$select=Indicador,Data,DataReferencia,Media,Mediana,DesvioPadrao,Minimo,Maximo"
        urlAnual = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais?$top=10000&$filter=Data ge '{initial_date}' and Data le '{final_date}'&$format=text/csv&$select=Indicador,Data,DataReferencia,Media,Mediana,DesvioPadrao,Minimo,Maximo"
        urlTop5Anual = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoTop5Anuais?$top=10000&$filter=Data ge '{initial_date}' and Data le '{final_date}'&$format=text/csv&$select=Indicador,Data,DataReferencia,Media,Mediana,DesvioPadrao,Minimo,Maximo,tipoCalculo"
        url12Meses = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoInflacao12Meses?$top=10000&$filter=Data ge '{initial_date}' and Data le '{final_date}'&$format=text/csv&$select=Indicador,Data,Suavizada,Media,Mediana,DesvioPadrao,Minimo,Maximo"

        urls = (['Mensal', urlMensal], ['Anuais', urlAnual], ['Top 5 Anuais', urlTop5Anual], ['12 Meses', url12Meses],)

        for file_name, url in (urls):
            response = self.use_requests(url=url, method='get')

            if response.status_code == 200:
                content = response.content
                self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=f'{file_name}.csv')            
            else: 
                raise f'Erro ao realizar a captura do arquivo {file_name}.csv.'