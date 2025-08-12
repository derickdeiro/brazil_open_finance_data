import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from projects.core.data_acquisition import TransformData
import xmltodict
import json
import pandas as pd
import re
from projects.data_acquisition.fre.posicao_acionaria.posicao_acionaria import transform_posicao_acionaria
from projects.data_acquisition.fre.posicao_acionaria.data_contract import fre_posicao_acionaria_schema
from projects.data_acquisition.fre.conselho_diretoria.conselho_diretoria import transform_conselho_diretoria_cor_genero, transform_conselho_diretoria_comp_conselho, transform_conselho_diretoria_comite
from projects.data_acquisition.fre.conselho_diretoria.data_contract import fre_descricao_caracteristicas_schema, fre_experiencia_profissional_schema, fre_composicao_comite_schema
from projects.data_acquisition.fre.remuneracao.remuneracao import transform_remuneracao_min_med_max, transform_remuneracao_total_orgao, transform_remuneracao_variavel
from projects.data_acquisition.fre.remuneracao.data_contract import fre_remun_min_med_max_schema, fre_remun_orgao_schema, fre_remun_variavel_schema
import pandera as pa
from typing import Dict, List


class TransformFRE(TransformData):
    def __init__(self):
        super().__init__()
        
    def transform_data(self, blob_path: str, exec_date: str) -> List[Dict]:
        """
        This method is responsible for transforming the raw data into the default layout.
        
        Args:
            blob_path (str): The path to the raw data.
            exec_date (str): The execution date.
        """
        
        fre_data_list = []
        
        for blob_name in blob_path:
            raw_data = self.download_raw_data(blob_name)
            file_name = self.create_file_name(raw_data)
            fre_json = self.get_fre_node(raw_data)
            
            fre_data = {'file_name': file_name,
                        'content': fre_json,}
            
            fre_data_list.append(fre_data)
        
        output_list = []
        for data in fre_data_list:    
            fre_data = data['content']
            file_name = data['file_name']

            transformed_data = self.get_each_fre_node_data(fre_data, file_name)
        
            for data in transformed_data:
                file_name = data['file_name']
                df_transformed = data['dataframe']
                schema = data['schema']
            
                output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=file_name, schema=schema)
                
                output_dict = self.upload_output(output_data=output_data)
                
                output_list.append(output_dict)
            
        return output_list
    
    
    def _convert_raw_xml_to_json(self, xml_content: str) -> Dict:
        """
        Convert the raw XML data to JSON format.
        
        Args:
            raw_data (str): The raw XML data.
        
        Returns:
            dict: The converted JSON data.
        """
          
        xml_content = re.sub(r'<ImagemObjetoArquivoPdf>[-A-Za-z0-9+/]*={0,3}</ImagemObjetoArquivoPdf>', '<ImagemObjetoArquivoPdf>{PDF}</ImagemObjetoArquivoPdf>', xml_content)
        
        data_dict = xmltodict.parse(xml_content)
        json_data = json.loads(json.dumps(data_dict))
        
        return json_data
    
    def create_file_name(self, xml_content: str) -> str:
        
        json_data = self._convert_raw_xml_to_json(xml_content)
        
        cnpj = json_data['XmlFormularioReferencia']['DadosEmpresa']['CnpjEmpresa']
        year = json_data['XmlFormularioReferencia']['DadosFRE']['DataReferencia'][-4:]
        doc_version = json_data['XmlFormularioReferencia']['Documento']['VersaoDocumento']
        
        return f'{cnpj}_{year}_{doc_version}'
        
        
    def get_fre_node(self, xml_content: str) -> Dict:
        """
        Get the FRE node from the XML content.
        
        Args:
            xml_content (str): The XML content.
        
        Returns:
            dict: The FRE node.
        """
        json_data = self._convert_raw_xml_to_json(xml_content)        
        fre_data = json_data['XmlFormularioReferencia']['DadosFRE']['Formulario']
        
        return fre_data
    
    
    def _create_fre_dict(self, dataframe: pd.DataFrame, schema: pa.DataFrameSchema, file_name: str, file_name_suffix: str) -> Dict:
        """
        Create a dictionary with the transformed data.
        
        Args: 
            dataframe (pd.DataFrame): The transformed dataframe.
            schema (pa.DataFrameSchema): The schema for the dataframe.
            file_name (str): The name of the file.
            file_name_suffix (str): The suffix for the file name.
            
        Returns:
            dict: The dictionary with the transformed data.
        """
        if dataframe.empty:
            return None
        
        temp_dict = {
                    'dataframe': dataframe, 
                    'schema': schema,
                    'file_name': f'{file_name_suffix}_{file_name}'
                    }
        
        return temp_dict
    
    
    def get_each_fre_node_data(self, fre_data: Dict, file_name: str) -> List[Dict]:
        """
        Get the data for each FRE node.
        
        Args:
            fre_data (dict): The JSON data to be transformed.
            file_name (str): The name of the file.
        
        Returns:
            dict: The transformed data.
        """
        
        fre_posicao_acionaria_dict = self._create_fre_dict(dataframe=transform_posicao_acionaria(fre_data), 
                                                       schema=fre_posicao_acionaria_schema, 
                                                       file_name=file_name,
                                                       file_name_suffix='posicao_acionaria')


        fre_remun_min_med_max_dict = self._create_fre_dict(dataframe=transform_remuneracao_min_med_max(fre_data),
                                                       schema=fre_remun_min_med_max_schema,
                                                       file_name=file_name,
                                                       file_name_suffix='remuneracao_min_med_max')
        
        fre_remun_orgao_dict = self._create_fre_dict(dataframe=transform_remuneracao_total_orgao(fre_data),
                                                 schema=fre_remun_orgao_schema,
                                                 file_name=file_name,
                                                 file_name_suffix='remuneracao_orgao')
        
        fre_remun_variavel_dict = self._create_fre_dict(dataframe=transform_remuneracao_variavel(fre_data),
                                                    schema=fre_remun_variavel_schema,
                                                    file_name=file_name,
                                                    file_name_suffix='remuneracao_variavel')

      
        fre_descricao_caracteristicas_dict = self._create_fre_dict(dataframe=transform_conselho_diretoria_cor_genero(fre_data),
                                                              schema=fre_descricao_caracteristicas_schema,
                                                              file_name=file_name,
                                                              file_name_suffix='descricao_caracteristicas')
        
        fre_experiencia_profissional_dict = self._create_fre_dict(dataframe=transform_conselho_diretoria_comp_conselho(fre_data),
                                                             schema=fre_experiencia_profissional_schema,
                                                             file_name=file_name,
                                                             file_name_suffix='experiencia_profissional')
        
        fre_composicao_comite_dict = self._create_fre_dict(dataframe=transform_conselho_diretoria_comite(fre_data),
                                                      schema=fre_composicao_comite_schema,
                                                      file_name=file_name,
                                                      file_name_suffix='composicao_comite')
    
        checklist = [
            fre_posicao_acionaria_dict,
            fre_remun_min_med_max_dict,
            fre_remun_orgao_dict,
            fre_remun_variavel_dict,
            fre_descricao_caracteristicas_dict,
            fre_experiencia_profissional_dict,
            fre_composicao_comite_dict,
            ]
        
        valid_checklist = [item for item in checklist if item is not None]
        
        return valid_checklist
 