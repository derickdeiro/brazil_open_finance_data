import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.bcb_ptax.constants import SOURCE_NAME


class ExtractBCBPtax(ExtractData):
    def __init__(self):
        super().__init__()
        
    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo Excel da ECB Taxa de Câmbio Euro
        
        """
        
        execution_date = exec_date.strftime('%Y%m%d')
        
        url = f"https://www4.bcb.gov.br/Download/fechamento/{execution_date}.csv"

        response = self.use_requests(url)
        
        if response.status_code == 200:
            content = response.content
            self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=f'bcb_ptax_{execution_date}.csv')
        
        else:
            raise Exception(f"Failed to download data for {execution_date}. Status code: {response.status_code}")
        