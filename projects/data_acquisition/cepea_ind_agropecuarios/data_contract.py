import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

cepea_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(100247), 
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
            nullable=False,
        ),
        'FAMCOMPL': pa.Column(
            str,
            pa.Check.equal_to('Geral'),
            nullable=False,
            coerce=True,
        ),
        'INTERVALO': pa.Column(
            int, 
            pa.Check.equal_to(1), 
            nullable=False, 
            coerce=True
        ),        
        'a206': pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        'a107': pa.Column(
            float,
            nullable=False,
            coerce=True,
        )
    }
)
