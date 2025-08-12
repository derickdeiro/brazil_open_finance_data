import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

fgv_di_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(10020), nullable=False, coerce=True
        ),
        'SERIE': pa.Column(str, pa.Check.isin(['IGP-DI - Final',
                                               'INCC-DI - Final',
                                               'IPA-DI - Final',
                                               'IPC-DI - Final',]), nullable=False, coerce=True),
        'DATASER': pa.Column(
            pa.String, 
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=False,
        ),
        'INTERVALO': pa.Column(
            int, pa.Check.equal_to(4), nullable=False, coerce=True
        ),
        'FAMCOMPL': pa.Column(
            str,
            pa.Check.equal_to('FGV Ibre - Índices'),
            nullable=False,
            coerce=True,
        ),
        'a25': pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        'a27': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
    }
)

fgv_index_m_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(10020), nullable=False, coerce=True
        ),
        'SERIE': pa.Column(str, pa.Check.isin(['IGP-M - Final',
                                               'INCC-M - Final',
                                               'IPA-M - Final',
                                               'IPC-M - Final',]), nullable=False, coerce=True),
        'DATASER': pa.Column(
            pa.String, 
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=False,
        ),
        'INTERVALO': pa.Column(
            int, pa.Check.equal_to(4), nullable=False, coerce=True
        ),
        'FAMCOMPL': pa.Column(
            str,
            pa.Check.equal_to('FGV Ibre - Índices'),
            nullable=False,
            coerce=True,
        ),
        'a25': pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        'a27': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
    }
)

fgv_index_10_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(10020), nullable=False, coerce=True
        ),
        'SERIE': pa.Column(str, pa.Check.isin(['IGP-10 - Final',
                                               'INCC-10 - Final',
                                               'IPA-10 - Final',
                                               'IPC-10 - Final',]), nullable=False, coerce=True),
        'DATASER': pa.Column(
            pa.String, 
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=False,
        ),
        'INTERVALO': pa.Column(
            int, pa.Check.equal_to(4), nullable=False, coerce=True
        ),
        'FAMCOMPL': pa.Column(
            str,
            pa.Check.equal_to('FGV Ibre - Índices'),
            nullable=False,
            coerce=True,
        ),
        'a25': pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        'a27': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
    }
)