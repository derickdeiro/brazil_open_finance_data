import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.fgv_indexes.constants import SOURCE_NAME

import requests
import ssl
import urllib3
from fake_useragent import UserAgent
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import List, Dict


class ExtractFGVIndex(ExtractData):
    def __init__(self) -> None:
        super().__init__()

    def extract_data(self, exec_date) -> str:
        """
        This method is responsible for extracting the raw data from the source.

        Args:
            blob_path (str): The path to the raw data.

        Returns:
            str: The path to the extracted raw data.
        """
        ua = UserAgent()
        """Download FGV files with proper SSL handling"""
        session = self.create_session_with_ssl_context()
        
        headers = {
            'User-Agent': ua.random,
            'Accept': 'application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Referer': 'https://portalibre.fgv.br/',
        }

        index_data = self.define_custom_data_by_date(exec_date)
        
        for data in index_data:
        
            index = data['index']
            full_year = data['full_year']
            numeric_month = data['numeric_month']
            short_month = data['short_month']
            
            url = f'https://portalibre.fgv.br/system/files/divulgacao/noticias/mat-complementar/{full_year}-{numeric_month}/{index}_FGV_complemento_{short_month}{full_year[-2:]}.xls'

            try:
                response = session.get(url, headers=headers, timeout=(15, 45))
                
                if response.status_code == 200:
                    content = response.content
                    self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=url)

            except Exception as e:
                print(f"❌ Error: {str(e)}")
                return False, None
        
    @staticmethod
    def create_session_with_ssl_context():
        """Create a session with custom SSL context to handle SSL issues"""
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        session = requests.Session()
        
        # Create a custom SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.set_ciphers('DEFAULT@SECLEVEL=1')  # Lower security level for compatibility
        
        # Create an HTTPAdapter with the custom SSL context
        class SSLAdapter(HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                kwargs['ssl_context'] = ssl_context
                return super().init_poolmanager(*args, **kwargs)
        
        # Setup retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = SSLAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session
    
    
    @staticmethod
    def define_custom_data_by_date(exec_date: datetime) -> List[Dict]:
        """
        Define custom data based on the execution date.
        This method generates a list of dictionaries containing index information
        for IGP-M, IGP-DI, and IGP-10. 

        Args:
            execution_date (datetime): The date of execution.

        Returns:
            List[Dict]: A list of dictionaries with index data.
        """
       
        numeric_month = exec_date.strftime('%m')
        full_year = exec_date.strftime('%Y')
            
        short_month = exec_date.strftime('%b').title()
        short_month_di = (exec_date - timedelta(days=32)).strftime('%b').title()

        index_data = [{'index': 'IGP-M',
                    'full_year': full_year,
                    'numeric_month': numeric_month,
                    'short_month': short_month},
                    {'index': 'IGP-DI',
                    'full_year': full_year,
                    'numeric_month': numeric_month,
                    'short_month': short_month_di},
                    {'index': 'IGP-10',
                    'full_year': full_year,
                    'numeric_month': numeric_month,
                    'short_month': short_month}]
        
        return index_data