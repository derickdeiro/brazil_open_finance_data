import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from typing import List
from projects.data_acquisition.anbima_fundos.constants import  SOURCE_NAME
from projects.core.data_acquisition import TransformData
from projects.data_acquisition.anbima_fundos.data_contract import anbima_fundos_schema
import pandas as pd
from pendulum import datetime


class TransformAnbimaFundos(TransformData):
    def __init__(self) -> None:
        super().__init__()

    def transform_data(
        self, blob_path: str, exec_date: str
    ) -> List[pd.DataFrame]:
        """
        This method is responsible for transforming the raw data into the default layout.
        
        Args:
            blob_path (str): The path to the raw data.
            exec_date (str): The execution date.
        """
        df_concat = pd.DataFrame()
        
        for path in blob_path:
            raw_data = self.download_raw_data(raw_data_path=path)
            df_temp = self.achatamento_retorno_api(raw_data)
            df_concat = pd.concat([df_concat, df_temp], ignore_index=True)
        df_transformed = df_concat.map(self.remove_espacamento)

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier="anbima_fundos", schema=anbima_fundos_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict

    @staticmethod
    def rename_key(d, old_key, new_key):
        d[new_key] = d[old_key]
        del d[old_key]

    @staticmethod
    def remove_identificador(lista_colunas, tipo_campo):
        lista_identificadores = []

        for identificador in lista_colunas:
            if identificador != tipo_campo:
                lista_identificadores.append(identificador)

        return lista_identificadores

    @staticmethod
    def remove_espacamento(campo_dado):
        if isinstance(campo_dado, str):
            campo_dado = campo_dado.strip()
            return campo_dado

    def achatamento_retorno_api(self, conteudo):
        #fundos 
        for index, i in enumerate(conteudo['content']): 

            campos_fundo = conteudo['content'][index]
            campos_rename_fundo = [
                campo for campo in campos_fundo.keys()
                if 'fundo' not in campo and campo != 'classes'
            ]

            for campo_fundo in campos_rename_fundo:
                self.rename_key(campos_fundo, campo_fundo, f'{campo_fundo}_fundo')
        
            #classes
            for j, retorno_j in enumerate(conteudo['content'][index]['classes']):
                campos_classes = conteudo['content'][index]['classes']
                campos_rename_classes = [
                    campo for campo in campos_classes[j].keys()
                    if 'classe' not in campo and campo != 'subclasses'
                ]

                for campo_classe in campos_rename_classes:
                    self.rename_key(campos_classes[j], campo_classe, f'{campo_classe}_classe')


                #subclasses
                if isinstance(conteudo['content'][index]['classes'][j].get('subclasses'), list):
                    for h, retorno_h in enumerate(conteudo['content'][index]['classes'][j]['subclasses']):
                        campos_subclasses = conteudo['content'][index]['classes'][j]['subclasses']
                        campos_rename_subclasses = [
                            campo for campo in campos_subclasses[h].keys()
                            if 'subclasse' not in campo and campo != 'subclasse'
                        ]

                        for campo_subclasse in campos_rename_subclasses:
                            self.rename_key(campos_subclasses[h], campo_subclasse, f'{campo_subclasse}_subclasse')
                else: 
                    continue


        df = pd.json_normalize(conteudo['content'], 'classes', self.remove_identificador(conteudo['content'][0].keys(), 'classes'))
        
        return df
    