import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.ecb_taxa_cambio_euro.constants import SOURCE_NAME
import zipfile
from io import BytesIO
import pandas as pd


class ExtractECBTaxaCambioEuro(ExtractData):
    def __init__(self):
        super().__init__()
        
    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo Excel da ECB Taxa de Câmbio Euro
        
        """
        
        url = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref.zip?8cbfec2785793c40ac00dbac520c1350'
        
        response = self.use_requests(url=url)
        
        if response.status_code == 200:
            zipped_content = zipfile.ZipFile(BytesIO(response.content))
            file_name = zipped_content.namelist()[0]
            
            df_temp = pd.read_csv(zipped_content.open(file_name))
            
            content = BytesIO()
            df_temp.to_csv(content, index=False)
            content.seek(0)
            
            # with zipped_content.open(file_name) as zip_file:
            #     content = zip_file.read().decode('utf-8')        
            
            self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name='ecb_taxa_cambio_euro.csv')
        