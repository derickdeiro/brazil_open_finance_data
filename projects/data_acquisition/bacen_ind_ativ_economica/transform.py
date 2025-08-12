import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

from projects.core.data_acquisition import TransformData
from projects.data_acquisition.bacen_ind_ativ_economica.data_contract import ind_ativ_economica_schema
from projects.data_acquisition.bacen_ind_ativ_economica.constants import (
    CODIGO_BACEN,
    DICT_SGS,
    ITF,
    FAMCOML,
    INTERVALO,
    ORIGINAL_COLUMNS,
    ATTRIBUTES_COLUMNS,
    SOURCE_NAME,
)
from io import BytesIO
import pandas as pd
import json


class TransformIndAtivEconomica(TransformData):
    def __init__(self):
        super().__init__()

    def transform_data(self, blob_path, exec_date):
        """
        This method is responsible for transforming the raw data into the default layout.
        """

        df_all_dicts = self._download_each_json_data(blob_path=blob_path)

        df_cleaned = self.clean_data_up(
            dataframe=df_all_dicts
        )

        df_last_date = self._get_recent_dataser(df_completo=df_cleaned)

        df_transformed = self._transform_dataframe_to_default_layout(
            dataframe=df_last_date,
            itf=ITF,
            famcompl=FAMCOML,
            intervalo=INTERVALO,
            original_columns=ORIGINAL_COLUMNS,
            attributes_columns=ATTRIBUTES_COLUMNS,
        )

        output_data = self.create_output_dict(execution_date=exec_date, dataframe=df_transformed, identifier=ITF, schema=ind_ativ_economica_schema)
        
        output_dict = self.upload_output(output_data=output_data)

        return output_dict

    def _download_each_json_data(self, blob_path):
        """
        This method is responsible for downloading each JSON data from the blob storage.
        
        Args:
            blob_path (str): The path to the raw data.
            
        Returns:
            pd.DataFrame: The DataFrame containing all the JSON data.
                """
        df_completo = pd.DataFrame()
        for path in blob_path:
            for codigo in CODIGO_BACEN:
                string_code = str(codigo)
                if string_code in path:
                    # Initialize raw_data to avoid UnboundLocalError
                    raw_data = None

                    try:
                        raw_data = self.download_raw_data(raw_data_path=path)

                        # Ensure raw_data is not None before proceeding
                        if raw_data is None:
                            print(f'No data returned for path: {path}')
                            continue

                        # Debugging the type of raw_data
                        print(f'Type of raw_data: {type(raw_data)}')

                        # If raw_data is a list of dictionaries, handle it directly
                        if isinstance(raw_data, list) and all(
                            isinstance(i, dict) for i in raw_data
                        ):
                            # Convert list of dictionaries to JSON string
                            raw_data = json.dumps(raw_data)

                        # Handle BytesIO or bytes by decoding
                        elif isinstance(raw_data, (BytesIO, bytes)):
                            raw_data = raw_data.decode('utf-8')

                        # Now, ensure raw_data is a valid string for pd.read_json()
                        if isinstance(raw_data, str):
                            try:
                                df_temp = pd.read_json(
                                    raw_data, orient='records'
                                )
                                df_temp['SERIE'] = codigo
                                df_temp['SERIE'] = df_temp['SERIE'].apply(
                                    lambda x: DICT_SGS[x]
                                )

                                df_completo = pd.concat(
                                    [df_completo, df_temp], axis=0
                                )
                            except ValueError as e:
                                print(f'Error reading JSON from raw_data: {e}')
                        else:
                            print(f'Unhandled raw_data type: {type(raw_data)}')

                    except Exception as e:
                        print(
                            f'Error occurred while downloading data for {path}: {e}'
                        )

        return df_completo

    def _get_recent_dataser(self, df_completo: pd.DataFrame):
        """
        This method is responsible for getting the most recent data for each serie.
        
        Args:
            df_completo (pd.DataFrame): The DataFrame containing all the data.
            
        Returns:
            pd.DataFrame: The DataFrame containing the most recent data for each serie.
        """
        lista_series = df_completo['SERIE'].unique()
        df_final = pd.DataFrame()
        for serie in lista_series:
            df_temp = df_completo.loc[df_completo['SERIE'] == serie]

            df_final = pd.concat([df_final, df_temp.tail(1)], axis=0)

        return df_final
