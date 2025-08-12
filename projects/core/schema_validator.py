import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))                

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaError
from datetime import datetime
from projects.core.data_acquisition import BlobManager

class SchemaValidator(BlobManager):
    def __init__(self):
        super().__init__()

    def validate_output_schema( 
        self, schema: pa.DataFrameSchema, output_path: str
    ):
        """
        Método que realiza a validação do Schema do Output.

        Args:
            schema (pa.DataFrameSchema): Schema a ser validado.
            output_path (str): Caminho do arquivo a ser validado.
        """ 

        dataframe = self.download_output_data(output_path=output_path)

        try:
            df = pd.read_csv(dataframe, sep='\t')
        except ValueError as json_error:
            raise ValueError(f'{json_error}')

        try:
            schema.validate(df, lazy=True)
        except SchemaError as schema_error:
            raise f'Falha na estrutura do Output {output_path}: {schema_error}'


def is_valid_date_format(series):
    """
    Método que realiza a validação do formato da data.

    Args:
        series (pd.Series): Série a ser validada.
    """  
    date_format = '%Y-%m-%d'

    try:
        valid = series.apply(
            lambda x: bool(datetime.strptime(x, date_format))
        )
        return valid.all()
    except ValueError:
        return False
