import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.fed_rates.constants import SOURCE_NAME
from datetime import datetime, timedelta

class ExtractFEDRates(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        This method is responsible for extracting the data from the source.
        Args:
            exec_date (str): The execution date.
        """
        
        start_date = exec_date - timedelta(days=1)
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')
        URLS = [{'file_name': 'avg_rates',
                 'url': f'https://markets.newyorkfed.org/read?startDt={start_date}&endDt={end_date}&eventCodes=525&productCode=50&sort=postDt:-1,eventCode:1&format=xlsx'},
                {'file_name': 'unsecured_rates',
                 'url': f'https://markets.newyorkfed.org/read?startDt={start_date}&endDt={end_date}&eventCodes=500,505&productCode=50&sort=postDt:-1,eventCode:1&format=xlsx'},
                {'file_name': 'secured_rates',
                 'url': f'https://markets.newyorkfed.org/read?startDt={start_date}&endDt={end_date}&eventCodes=510,515,520&productCode=50&sort=postDt:-1,eventCode:1&format=xlsx'},
                ]

        for url in URLS:
            file_name = url['file_name']
            url = url['url']
            
            response = self.use_requests(url)
            
            if response.status_code == 200:
                content = response.content
                self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=f'{file_name}.xlsx')
            else:
                raise Exception(f"Failed to fetch data from {url}. Status code: {response.status_code}")
        
            