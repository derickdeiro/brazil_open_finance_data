import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

bacen_parametros_circulares_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(100250), 
            nullable=False, 
            coerce=True
        ),
        'SERIE': pa.Column(
            str, 
            nullable=False,
            coerce=True
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
            int, pa.Check.equal_to(1), 
            nullable=False, 
            coerce=True
        ),
        'a24': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
    }
)
