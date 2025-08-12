import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.data_acquisition.cvm_funds.constants import (
    ITF,
    INTERVALO,
    FAMCOML,
    DICT_CLASSE,
    DICT_FUNDO,
    DICT_SUBCLASSE,
    DICT_DIARIO,
)
from projects.data_acquisition.cvm_funds.data_contract import (
    fundo_classe_subclasse_schema,
    fundo_classe_schema,
    fundo_schema,
    diario_schema,
    )
from projects.core.data_acquisition import TransformData
import pandas as pd
import duckdb
import re
from io import BytesIO

class TransformCVMFunds(TransformData):
    def __init__(self) -> None:
        super().__init__()

    def transform_data(
        self, blob_path: str, exec_date: str
    ) -> pd.DataFrame:
        """
        This method is responsible for transforming the raw data into the default layout.
        
        Args:
            blob_path (str): The path to the raw data.
            exec_date (str): The execution date.
        """

        data_funds_list = []
        
        for blob_name in blob_path:
            raw_data = self.download_raw_data(raw_data_path=blob_name)
            
            original_name = os.path.basename(blob_name)
            file_name = original_name.split('.')[0]
        
            df_temp = pd.read_csv(raw_data, sep=';', encoding='latin1')
            temp_dict = {'file_name': file_name, 'data': df_temp}
            data_funds_list.append(temp_dict)

        for dictionary in data_funds_list:
            
            dataframe = dictionary['data']
            
            dataframe = self._define_data_types(dataframe)
            
            if 'inf_diario_fi' in dictionary['file_name']:
                df_diario = self._clean_df_diario(dataframe)
                df_diario.rename(columns=DICT_DIARIO, inplace=True)
            elif dictionary['file_name'] == 'registro_subclasse':
                df_subclasse = self._clean_subclasse_data(dataframe)
                df_subclasse.rename(columns=DICT_SUBCLASSE, inplace=True)
            elif dictionary['file_name'] == 'registro_fundo':
                df_fundo = self._clean_fundo_data(dataframe)
                dict_cnpj_tipo_fundo = df_fundo[['CNPJ_Fundo', 'Tipo_Fundo']].to_dict('list')
                df_fundo.rename(columns=DICT_FUNDO, inplace=True)
            elif dictionary['file_name'] == 'registro_classe':
                df_classe = self._clean_classe_data(dataframe)
                df_classe.rename(columns=DICT_CLASSE, inplace=True)

        dict_cnpj_tipo_fundo = dict(zip(dict_cnpj_tipo_fundo['CNPJ_Fundo'], dict_cnpj_tipo_fundo['Tipo_Fundo']))
        
        df_diario['TEMP'] = df_diario['CNPJ_FUNDO_CLASSE'].map(dict_cnpj_tipo_fundo)
        
        df_diario['TEMP'] = df_diario['TEMP'].fillna(df_diario['TP_FUNDO_CLASSE'])
        
        df_classe_subclasse_merged = pd.merge(df_classe, df_subclasse, on='c4145', how='left')

        df_classe_no_subclasse = duckdb.sql("SELECT * FROM df_classe WHERE c4145 NOT IN (SELECT c4145 FROM df_subclasse)").df()

        df_fundo_classe_subclasse_merged = pd.merge(df_classe_subclasse_merged, df_fundo, on='c312', how='left')

        df_fundo_no_classe = duckdb.sql("SELECT * FROM df_fundo WHERE c312 NOT IN (SELECT c312 FROM df_classe)").df()

        df_fundo_classe_merged = pd.merge(df_classe_no_subclasse, df_fundo, on='c312', how='left')   

        df_fundo_classe_subclasse_merged['SERIE'] = df_fundo_classe_subclasse_merged['c316'].astype(str) + '_' + df_fundo_classe_subclasse_merged['c264'].astype(str) + '_' + df_fundo_classe_subclasse_merged['c4234'].astype(str)
        df_fundo_classe_merged['SERIE'] = df_fundo_classe_merged['c316'].astype(str) + '_' + df_fundo_classe_merged['c264'].astype(str)
        df_fundo_no_classe['SERIE'] = df_fundo_no_classe['c316'].astype(str) + '_' + df_fundo_no_classe['c313'].astype(str)
        df_diario['SERIE'] = df_diario['TEMP'].astype(str) + '_' + df_diario['CNPJ_FUNDO_CLASSE'].astype(str) + '_' + df_diario['ID_SUBCLASSE'].astype(str)
        # df_diario['SERIE'] = df_diario['TP_FUNDO_CLASSE'].astype(str) + '_' + df_diario['CNPJ_FUNDO_CLASSE'].astype(str) + '_' + df_diario['ID_SUBCLASSE'].astype(str)

        df_diario = df_diario.drop(['TP_FUNDO_CLASSE', 'CNPJ_FUNDO_CLASSE', 'ID_SUBCLASSE', 'TEMP'], axis=1)

        final_dataframes = [{'dataframe': df_fundo_classe_subclasse_merged,
                            'schema': fundo_classe_subclasse_schema},
                            {'dataframe': df_fundo_classe_merged,
                            'schema': fundo_classe_schema},
                            {'dataframe': df_fundo_no_classe,
                            'schema': fundo_schema},
                            {'dataframe': df_diario,
                            'schema': diario_schema},
                            ]

        output_list = []
        
        for cont, df in enumerate(final_dataframes):
            dataframe = df['dataframe']
            dataframe = self._clean_serie_column(dataframe)
            
            if 'DATASER' not in dataframe.columns:
                dataframe['DATASER'] = exec_date.strftime('%Y-%m-%d')
            
            df_transformed = self._transform_dataframe_to_default_layout(
                dataframe=dataframe,
                itf=ITF,
                famcompl=FAMCOML,
                intervalo=INTERVALO,
            )
            for column in df_transformed.columns:
                df_transformed[column] = df_transformed[column].astype(str).replace('nan_', '', regex=True)
                df_transformed[column] = df_transformed[column].astype(str).replace('nan', '', regex=True)
                df_transformed[column] = df_transformed[column].astype(str).replace('NaT', '', regex=True)

            output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=df['schema'], cont=cont)
            
            output_dict = self.upload_output(output_data=output_data)
            
            output_list.append(output_dict)

        return output_list
    
    def _define_data_types(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Define the data types of the dataframe columns
        
        Args:
        dataframe: pd.DataFrame - Dataframe to be cleaned
        
        Returns:
        pd.DataFrame - Dataframe with the data types defined
        
        """
        string_columns = ['Administrador', 'Auditor', 'CNPJ_Administrador', 'CNPJ_Auditor', 'CNPJ_Controlador', 'CNPJ_Custodiante', 'CNPJ_FUNDO_CLASSE', 'CNPJ_Fundo',
                        'CPF_CNPJ_Gestor', 'Classe_Cotas', 'Classe_ESG', 'Classificacao', 'Classificacao_Anbima', 'Controlador', 'Custodiante', 'Denominacao_Social',
                        'Diretor', 'Entidade_Investimento', 'Exclusivo', 'Forma_Condominio', 'Gestor', 'ID_Subclasse', 'Indicador_Desempenho','Permitido_Aplicacao_CemPorCento_Exterior',
                        'Publico_Alvo', 'Situacao', 'TP_FUNDO_CLASSE', 'Tipo_Classe', 'Tipo_Fundo', 'Tipo_Pessoa_Gestor',
                        'Tributacao_Longo_Prazo']

        int_columns = ['Codigo_CVM', 'ID_Registro_Classe', 'ID_Registro_Fundo', 'NR_COTST']

        date_columns = ['DATASER', 'Data_Adaptacao_RCVM175', 'Data_Cancelamento', 'Data_Constituicao', 'Data_Fim_Exercicio_Social', 'Data_Inicio', 'Data_Inicio_Exercicio_Social',
                        'Data_Inicio_Situacao', 'Data_Patrimonio_Liquido', 'Data_Registro']

        float_columns = ['CAPTC_DIA', 'RESG_DIA', 'VL_PATRIM_LIQ', 'VL_QUOTA', 'VL_TOTAL']

        for column in dataframe.columns:
            if column in string_columns:
                dataframe[column] = dataframe[column].astype(str)
                dataframe[column] = dataframe[column].apply(self._remove_illegal_characters)
            elif column in int_columns:
                dataframe[column] = dataframe[column].fillna(0)
                dataframe[column] = dataframe[column].astype(int)
            elif column in date_columns:
                dataframe[column] = pd.to_datetime(dataframe[column], errors='coerce')
            elif column in float_columns:
                dataframe[column] = dataframe[column].astype(float)
                
        return dataframe
    
    def _clean_subclasse_data(self, df_subclasse: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the SUBCLASSE dataframe
        
        Args:
        df_subclasse: pd.DataFrame - SUBCLASSE dataframe to be cleaned
        
        Returns:
        pd.DataFrame - Cleaned SUBCLASSE dataframe
        """
    
        df_subclasse = duckdb.sql("SELECT * FROM df_subclasse WHERE Situacao <> 'Em Análise'").df()

        return df_subclasse


    def _clean_fundo_data(self, df_fundo: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the FUNDO dataframe
        
        Args:
        df_fundo: pd.DataFrame - FUNDO dataframe to be cleaned
        
        Returns:
        pd.DataFrame - Cleaned FUNDO dataframe
        """
        
        df_fundo = duckdb.sql("SELECT * FROM df_fundo WHERE Situacao NOT IN ('Em Análise', 'Cancelado')").df()
        
        df_fundo['CNPJ_Fundo'] = df_fundo['CNPJ_Fundo'].apply(self._fill_cnpj_length)

        df_fundo.drop('Patrimonio_Liquido', axis=1, inplace=True)
        
        df_fundo_unique = duckdb.sql("""SELECT DISTINCT ON (ID_Registro_Fundo) *
                                    FROM df_fundo
                                    ORDER BY ID_Registro_Fundo, Data_Inicio_Situacao DESC
                                """).df()
        
        return df_fundo_unique


    def _clean_classe_data(self, df_classe: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the CLASSE dataframe
        
        Args:
        df_classe: pd.DataFrame - CLASSE dataframe to be cleaned
        
        Returns:
        pd.DataFrame - Cleaned CLASSE dataframe
        """

        df_classe['CNPJ_Classe'] = df_classe['CNPJ_Classe'].apply(self._fill_cnpj_length)
        
        return df_classe


    def _clean_df_diario(self, df_diario: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the DIARIO dataframe
        
        Args:
        df_diario: pd.DataFrame - DIARIO dataframe to be cleaned
        
        Returns:
        pd.DataFrame - Cleaned DIARIO dataframe
        """
        
        # df_diario['TP_FUNDO_CLASSE'] = df_diario['TP_FUNDO_CLASSE'].astype(str).replace('CLASSES - FIF', 'FIF', regex=True)
        
        df_diario['CNPJ_FUNDO_CLASSE'] = df_diario['CNPJ_FUNDO_CLASSE'].apply(lambda x: ''.join(c for c in str(x) if c.isdigit()))
        
        df_diario['a84'] = df_diario[ 'CAPTC_DIA'].astype(float) - df_diario['RESG_DIA'].astype(float)

        return df_diario


    def _fill_cnpj_length(self, cnpj: int) -> str:
        """
        Fill the CNPJ with zeros to have a length of 14 characters
        
        Args:
        cnpj: int - CNPJ to be filled
        
        Returns:
        str - CNPJ with 14 characters
        """
        cnpj = str(int(cnpj))
        if len(cnpj) < 14:
            return '0'*(14-len(cnpj)) + cnpj
        return cnpj


    def _remove_illegal_characters(self, value: str) -> str:
        """
        Remove illegal characters from a string
        
        Args:
        value: str - String to be cleaned
        
        Returns:
        str - Cleaned string
        
        """
        illegal_characters_pattern = re.compile(r'[\x00-\x1F\x7F-\x9F]')
        return illegal_characters_pattern.sub('', value)


    def _clean_serie_column(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the SERIE column of the dataframe
        
        Args:
        dataframe: pd.DataFrame - Dataframe to be cleaned
        
        Returns:
        pd.DataFrame - Dataframe with the SERIE column cleaned
        
        """
        dataframe['SERIE'] = dataframe['SERIE'].replace('_nan', '', regex=True)
        return dataframe
    