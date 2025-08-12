import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

anbima_debentures_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(100101), nullable=False, coerce=True
        ),
        'SERIE': pa.Column(str, nullable=False, coerce=True),
        'DATASER': pa.Column(
            pa.String, 
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=False, 
        ),
        'INTERVALO': pa.Column(
            int, pa.Check.equal_to(1), nullable=False, coerce=True
        ),
        'FAMCOMPL': pa.Column(
            str,
            pa.Check.equal_to('Geral'),
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
            nullable=False,
            coerce=True,
        ),
        'c606': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'c1002': pa.Column(
            str,
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=False,
            coerce=True,
        ),
        'c189': pa.Column(
            str,
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=True,
            coerce=True,
        ),
        'a5': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'a30': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'a265': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'a9': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'a17': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'a15': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'a63': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'a78': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'a55': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
    }
)
