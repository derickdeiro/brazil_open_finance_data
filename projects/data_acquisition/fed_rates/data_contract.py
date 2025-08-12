import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

fed_rates_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(100239), nullable=False, coerce=True
        ),
        'SERIE': pa.Column(str, pa.Check.isin(['SOFR AVG 30D',
                                               'SOFR AVG 90D', 
                                               'SOFR AVG 180D',
                                               'SOFR (INDEX)',
                                               'EFFR (Unsecured Overnight Financing Rate)',
                                               'OBFR (Unsecured Overnight Financing Rate)',
                                               'TGCR (Secured Overnight Financing Rate)',
                                               'BGCR (Secured Overnight Financing Rate)',
                                               'SOFR (Secured Overnight Financing Rate)']), nullable=False, coerce=True),
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
        'a650': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=True,
            coerce=True,
        ),
        'a651': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=True,
            coerce=True,
        ),
        'a652': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=True,
            coerce=True,
        ),
        'a653': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=True,
            coerce=True,
        ),
        'a654': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=True,
            coerce=True,
        ),
    }
)
