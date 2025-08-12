import os
import sys
import re

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))       

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.bndes_moedas_contratuais.constants import SOURCE_NAME

class ExtractMoedasContratuais(ExtractData):
    def __init__(self):
        super().__init__()
        
    def extract_data(self, exec_date):
        """
        This method is responsible for extracting the raw data from the source.
        
        Args:
            exec_date (str): The execution
            
        Returns:    
            list: The list of raw data.
        """
    
        url_all_currencies = 'https://www.bndes.gov.br/SiteBNDES/bndes/bndes_pt/Galerias/Convivencia/Moedas_Contratuais/index.html'
        response = self.use_requests(url=url_all_currencies, method='get')
        soup = self.use_soup(content=response.content)
        
        select = soup.find('select', id='Moeda')
        options = select.find_all('option')
        
        raw_data_list = []
        for option in options:
            option_value = option.get('value')
            if re.search(r'\(([^)]+)\)', option.text):
                option_text = option.text.replace(f'({option_value})', '').strip()
                url_data = f'https://www.bndes.gov.br/Moedas/um{option_value}.txt'
            else:
                option_text = option.text.strip()
                url_data = f'https://www.bndes.gov.br/apoioFinanceiro/rest/taxas/{option_value}.txt'
            
            response = self.use_requests(url=url_data)
            content = response.content
            
            raw_data = self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=f'{option_text}.txt')
            raw_data_list.append(raw_data)
            
        return raw_data_list