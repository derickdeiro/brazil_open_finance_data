import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.focus.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
)
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.focus.data_contract import focus_schema
import pandas as pd


class TransformFocus(TransformData):
    def __init__(self) -> None:
        super().__init__()

    def transform_data(
        self, blob_path: str, exec_date: str
    ) -> List[pd.DataFrame]:

        df_output = pd.DataFrame()

        for file in blob_path:
            file_name = os.path.basename(file)
            file_name = file_name.split('.')[0]

            raw_data = self.download_raw_data(raw_data_path=file)            


            df_raw = pd.read_csv(raw_data)
            df_cleaned = self.clean_data_up(dataframe=df_raw)
            df_formated = self._transform_standard(dataframe=df_cleaned, filename=file_name, exec_date=exec_date)

            df_output = pd.concat([df_output, df_formated], axis=0)


        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=df_output,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=focus_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict


    def _transform_standard(self, dataframe: pd.DataFrame, filename: str, exec_date: str) -> pd.DataFrame:
        df_temp = dataframe.copy()
        index = len(df_temp)
        df_temp['ID'] = [100030] * index

        if filename == 'Top 5 Anuais':
            df_temp['SERIE'] = list(map(lambda prazo, indicador, dataRef: f"Focus TOP 5 {'CP' if prazo == 'C' else 'MP' if prazo == 'M' else 'LP'} {indicador} {dataRef}",
                                        df_temp['tipoCalculo'], 
                                        df_temp['Indicador'],
                                        df_temp['DataReferencia']))
        elif filename == '12 Meses':
            df_temp['SERIE'] = list(map(lambda indicador, suavizada: f"Focus {indicador} {'Suavizada' if suavizada == 'S' else ''}", 
                                        df_temp['Indicador'],
                                        df_temp['Suavizada']))
        elif filename == 'Mensal':
            df_temp['SERIE'] = list(map(lambda indicador, dataRef: f"Focus {indicador} {exec_date.strptime(dataRef, '%m/%Y').strftime('%b %Y')}",
                                        df_temp['Indicador'],
                                        df_temp['DataReferencia']))
        else:
            df_temp['SERIE'] = list(map(lambda indicador, dataRef: f"Focus {indicador} {dataRef}",
                                        df_temp['Indicador'],
                                        df_temp['DataReferencia']))        
        df_temp['INTERVALO'] = [1] * index
        df_temp['FAMCOMPL'] = ['GERAL'] * index
        df_temp['DATASER'] = df_temp['Data']
        df_temp['a9']  = list(map(lambda x: str(x).replace(',','.'), df_temp['DesvioPadrao']))
        df_temp['a14'] = list(map(lambda x: str(x).replace(',','.'), df_temp['Mediana']))
        df_temp['a15'] = list(map(lambda x: str(x).replace(',','.'), df_temp['Maximo']))
        df_temp['a16'] = list(map(lambda x: str(x).replace(',','.'), df_temp['Media']))
        df_temp['a17'] = list(map(lambda x: str(x).replace(',','.'), df_temp['Minimo']))

        return df_temp