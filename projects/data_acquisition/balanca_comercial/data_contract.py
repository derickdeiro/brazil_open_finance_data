import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

balanca_comercial_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(10278), nullable=False, coerce=True
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
        'c6009': pa.Column(
            str,
            pa.Check.isin(['EXP', 'IMP']),
            nullable=False,
            coerce=True,
        ),
        'c194': pa.Column(
            int,
            pa.Check.greater_than(0),
            nullable=False,
            coerce=True,
        ),
        'c4107': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c144': pa.Column(
            int,
            pa.Check.greater_than(0),
            nullable=True,
            coerce=True,
        ),
        'c4106': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c44': pa.Column(
            int,
            pa.Check.greater_than_or_equal_to(0),
            nullable=True,
            coerce=True,
        ),
        'c4105': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'a291': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=True,
            coerce=True,
        ),
        'a468': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=True,
            coerce=True,
        ),
    }
)
