import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

adr_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(10205), nullable=False, coerce=True
        ),
        'SERIE': pa.Column(str, nullable=False, coerce=True),
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
            pa.Check.equal_to('Geral'),
            nullable=False,
            coerce=True,
        ),
        'a9999': pa.Column(
            int,
            pa.Check.equal_to(1),
            nullable=False,
            coerce=True,
        ),
        'c25': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'c44': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c1051': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
    }
)
