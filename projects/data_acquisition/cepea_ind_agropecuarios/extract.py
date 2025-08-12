import os
import sys
import json
from typing import TypedDict, List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.cepea_ind_agropecuarios.constants import SOURCE_NAME, PROCUCTS_URL

class ExtractCepeaIndAgropecuarios(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):

        class LinkConfig(TypedDict):
            id: int
            serie: str
            name: str

        list_link_config: List[LinkConfig] = []
        list_link_config = json.loads(json.dumps(PROCUCTS_URL)) 

        for product in list_link_config:
            try:
                url = f"https://www.cepea.esalq.usp.br/br/indicador/series/{product['name']}.aspx?id={product['id']}"

                response = self.use_requests(url=url, method='get')

                if response.status_code == 200:
                    content = response.content

                    self.upload_raw_data(source_name=SOURCE_NAME, raw_data=content, dataser=exec_date, url_or_file_name=f'{product["serie"].replace("/", '_')}.xls')            
                else:
                    print(f"Error: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"Error downloading data for {product}: {e}")