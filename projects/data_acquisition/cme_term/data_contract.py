import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

cme_term_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(100165), nullable=False, coerce=True
        ),
        'SERIE': pa.Column(str, pa.Check.isin(['Term SOFR 1M', 'Term SOFR 3M', 'Term SOFR 6M', 'Term SOFR 12M']), nullable=False, coerce=True),
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
            pa.Check.equal_to('Geral'),
            nullable=False,
            coerce=True,
        ),
        'a23': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
    }
)
