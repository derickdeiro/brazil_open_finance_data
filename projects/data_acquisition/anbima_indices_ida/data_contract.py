import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

anbima_indices_ida_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(10042), nullable=False, coerce=True
        ),
        'SERIE': pa.Column(str, pa.Check.isin(['IDA - DI',
                                               'IDA - GERAL',
                                               'IDA - IPCA',
                                               'IDA - IPCA ex-Infraestrutura',
                                               'IDA - IPCA Infraestrutura',]), nullable=False, coerce=True),
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
        'a13': pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        'a131': pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        'a26': pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        'a27': pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        'a272': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'a28': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'a55': pa.Column(
            int,
            nullable=True,
            coerce=True,
        ),
        'a68': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'a87': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
    }
)
