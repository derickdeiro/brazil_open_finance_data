import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

bcb_pu550_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(100157), 
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
            nullable=False  # Ensures no null values are present
        ),
        'INTERVALO': pa.Column(
            int, 
            pa.Check.equal_to(1), 
            nullable=False, 
            coerce=True
        ),
        'c1': pa.Column(
            str,
            pa.Check.isin(["NTN-P","NTN-P","LTN","LFT","NTN-B1","NTN-B1","NTN-B","NTN-B","NTN-B","NTN-C","NTN-I","NTN-F","NTN-F","NTN-F","NTN-F",]),
            nullable=False,
            coerce=True,
        ),
        'c2': pa.Column(
            int, 
            pa.Check.isin([8, 9, 100000, 210100, 700000, 700001, 760197, 760198, 760199, 770100, 891300, 950197, 950198, 950199, 950200]),
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
        'a63': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'a121': pa.Column(
            int,
            pa.Check.equal_to(0),
            nullable=True,
            coerce=True,
        ),
        'a135': pa.Column(
            int,
            nullable=True,
            coerce=True,
        ),
    }
)
