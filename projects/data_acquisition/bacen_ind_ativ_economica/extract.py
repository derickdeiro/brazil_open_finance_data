import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import ExtractData
from projects.data_acquisition.bacen_ind_ativ_economica.constants import (
    CODIGO_BACEN,
    SOURCE_NAME,
)
from io import BytesIO
import pandas as pd
import logging


class ExtractIndAtivEconomica(ExtractData):
    def __init__(self):
        super().__init__()

    def extract_data(self, exec_date):
        """
        This method is responsible for extracting the raw data from the source.
        """

        raw_paths = []
        missing_codes = []
        for code in CODIGO_BACEN:
            try:
                url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados?formato=json'

                df_temp = pd.read_json(url)

                last_data = df_temp.tail(1)

                file_name = f'indice_atividade_economica_{code}.json'

                json_bytes = BytesIO()
                json_data = last_data.to_json(json_bytes, orient='records')
                json_bytes.seek(0)

                raw_path = self.upload_raw_data(
                    source_name=SOURCE_NAME,
                    raw_data=json_bytes,
                    dataser=exec_date,
                    url_or_file_name=file_name,
                )

                raw_paths.append(raw_path)

            except Exception as e:
                print(f'Erro ao extrair dados do código BACEN {code}: {e}')
                missing_codes.append(code)

        if len(missing_codes) > 0:
            logging.warning(f'Códigos não encontrados: {missing_codes}')
        else:
            return raw_paths
