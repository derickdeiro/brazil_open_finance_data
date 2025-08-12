from typing import List
from projects.core.data_acquisition import ExtractData
from io import BytesIO


class ExtractAnbimaPF(ExtractData):
    def __init__(self):
        self.web_site = 'https://www.anbima.com.br/'

    def extract_data(self):
        soup = self.use_soup(
            'https://www.anbima.com.br/pt_br/informar/estatisticas/varejo-private-e-gestores-de-patrimonio/private-consolidado-mensal.htm'
        )

        for element in soup.select('a[data-anbima-arquivo-contentid]'):
            if '.xlsx' in element['href']:
                url = element['href']
                break

        full_web_site = f'{self.web_site}{url}'
        excel_data = self.use_requests(full_web_site)
        raw_data = BytesIO(excel_data.content)

        # BUILDING
