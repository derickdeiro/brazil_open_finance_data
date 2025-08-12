import pandera.pandas as pa
from projects.data_acquisition.bacen_ind_ativ_economica.constants import NOME_SERIE
from projects.core.schema_validator import is_valid_date_format

ind_ativ_economica_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(10211), nullable=False, coerce=True
        ),
        'SERIE': pa.Column(
            str, pa.Check.isin(NOME_SERIE), nullable=False, coerce=True
        ),
        'DATASER': pa.Column(
            pa.String, 
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=False,  # Ensures no null values are present
        ),
        'INTERVALO': pa.Column(
            int, pa.Check.equal_to(4), nullable=False, coerce=True
        ),
        'FAMCOMPL': pa.Column(
            str,
            pa.Check.equal_to('BACEN - Índice de Atividade Econômica'),
            nullable=False,
            coerce=True,
        ),
        'a24': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
    }
)
