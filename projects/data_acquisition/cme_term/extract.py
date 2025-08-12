import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.cme_term.constants import SOURCE_NAME

class ExtractCMETerm(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        Função que faz o download do arquivo Excel da DR Universe
        """
        url_term: str = 'https://www.global-rates.com/en/interest-rates/cme-term-sofr/'
        response = self.use_requests(url=url_term, method='get')
        content = response.content
        soup = self.use_soup(content=content)

        content = self._get_term_data_from_html(soup=soup)
        
        self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name='cme_term.json')
            

    def _get_term_data_from_html(self, soup):
        """
        Função que faz o download do arquivo Excel da DR Universe
        """
        tr = soup.find_all('tr')

        items = []
        for item in tr:
            td_list = []
            for td in item:
                if td != '\n':
                    td_list.append(td.text.replace('\n', '').replace('\t', ''))
            items.append(td_list)
            
        term_values = []
        date_list = items[0]
        for lista in items[1:]:
            count = 0
            for item in lista:
                if count >= 1:
                    temp_dict = {'term_period': lista[0], 'date': date_list[count], 'rate': lista[count]}
                    term_values.append(temp_dict)
                count += 1

        return term_values