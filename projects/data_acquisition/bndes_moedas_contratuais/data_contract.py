import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

moedas_contratuais_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(90011345), nullable=False, coerce=True
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
            int, pa.Check.isin([1, 4, 6, 9]), nullable=False, coerce=True
        ),
        'FAMCOMPL': pa.Column(
            str,
            pa.Check.equal_to('FAM137'),
            nullable=False,
            coerce=True,
        ),
        'a24': pa.Column(
            float,
            pa.Check.greater_than(0),
            nullable=True,
            coerce=True,
        ),
        'c19': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c44': pa.Column(
            str,
            nullable=True,
            coerce=True,
        )
    }
)
