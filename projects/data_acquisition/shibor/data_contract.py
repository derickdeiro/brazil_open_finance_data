import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

shibor_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(10096), nullable=False, coerce=True
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
            int, pa.Check.equal_to(1), nullable=False, coerce=True
        ),
        'FAMCOMPL': pa.Column(
            str,
            pa.Check.equal_to('Shibor - Taxa de Juros'),
            nullable=False,
            coerce=True,
        ),
        'a589': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'a122': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'a123': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'a88': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'a90': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'a93': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'a126': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'a94': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
    }
)
