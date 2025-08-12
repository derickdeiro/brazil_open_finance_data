import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))       

from datetime import datetime as dt
from typing import Dict, List
from workadays import workdays as wd
from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.shibor.shibor_constants import SHIBOR_FILES, SOURCE_NAME


class ExtractShibor(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date) -> List[Dict]:
        """
        This method is responsible for extracting the raw data from the source.
        
        Args:
            exec_date (str): The execution
            
        Returns:
            list: The list of raw data.
        """
        
        execution_date_dt = exec_date.strftime('%Y-%m-%d')

        for shibor_tax in SHIBOR_FILES:
            try:
                content, file_name = self._get_files(
                    dataser=execution_date_dt, shibor_type=shibor_tax
                )
                self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=file_name)
                
            except Exception as e:
                raise (f'Erro ao capturar o arquivo: {shibor_tax}.', e)
        
    def _get_files(self, dataser: str, shibor_type: str):
        """
        This method is responsible for downloading the files from the source.
        
        Args:
            dataser (str): The date of the data.    
            shibor_type (str): The type of the shibor.
            
        Returns:
            tuple: The content of the file and the file name.
                
        """
        quotes = False
        if shibor_type == 'ShiborPriHisExcel':
            quotes = True

        payload = self._create_payload(dataser=dataser, quotes=quotes)

        url = f'https://www.shibor.org/dqs/rest/cm-u-bk-shibor/{shibor_type}'
        response = self.use_requests(url=url, method='post', data=payload)

        if response.status_code == 200:
            content = response.content
            file_name = f'{url}.xlsx'
            return content, file_name
        else:
            raise ValueError(f'Falha ao realizar o download. {response.status_code}')

    def _create_payload(self, dataser: str, quotes=False) -> dict:
        """
        This method is responsible for creating the payload to be used in the request.
        
        Args:
            dataser (str): The date of the data.
            quotes (bool): Whether to download the quotes or not.
            
        Returns:
            dict: The payload to be used in the request.
        """
        
        today = dt.today()
        if dataser == today.strftime('%Y-%m-%d'):
            data_date = wd.workdays(start_date=today, ndays=-1).strftime(
                '%d %b %Y'
            )
        else:
            data_date = dt.strptime(dataser, '%Y-%m-%d').strftime('%d %b %Y')

        payload = {
            'lang': 'en',
            'startDate': data_date,
            'endDate': data_date,
        }

        if quotes:
            payload[
                'memCode'
            ] = '|100000111000000101001|100000211000000101001|100000311000000101001|100000411000000101001|100000531000000102001|100000844030000102001|100000711000000102001|100000911000000102001|100001435010000102001|200001131000000102001|101000111000000104001|102000031000000104001|290000531000000106001|100001011000000102001|100001244010000102001|101050011000000208011|100005311000000103001|100001511000000102001'
            payload[
                'instnEnNm'
            ] = '|ICBC|ABC|BOC|CBC|BOCOM|CMB|CNCB|CEB|CIB|SPDB|BOB|BOS|HSBC|HXB|GDB|PSBC|CDB|CMSB'
            return payload

        else:
            return payload
