import pandera.pandas as pa
from datetime import datetime
from projects.core.schema_validator import is_valid_date_format

# def is_valid_date_format(self, series):
#     """
#     Método que realiza a validação do formato da data.

#     Args:
#         series (pd.Series): Série a ser validada.
#     """  
#     date_format = '%Y-%m-%d'

#     try:
#         valid = series.apply(
#             lambda x: bool(datetime.strptime(x, date_format))
#         )
#         return valid.all()
#     except ValueError:
#         return False

fundo_classe_subclasse_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(10235), nullable=False, coerce=True
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
            pa.Check.equal_to('Geral'),
            nullable=False,
            coerce=True,
        ),
        'c312': pa.Column(
            int,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'c4145': pa.Column(
            int,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'c264': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'c265': pa.Column(
            int,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'c4157': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c266': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c4146': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=False,
            coerce=True,
        ),
        'c277': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'a618': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'c289': pa.Column(
            str,
            pa.Check.isin(['Cancelado', 'Em Funcionamento Normal', 'Em Liquidação',
            'Fase Pré-Operacional', 'Em Análise']),
            nullable=False,
            coerce=True,
        ),
        'c290': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c291': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c5107': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c292': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c293': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c294': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c295': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c4152': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c296': pa.Column(
            str,
            pa.Check.isin(['Aberto', 'Fechado']),
            nullable=True,
            coerce=True,
        ),
        'c297': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c298': pa.Column(
            str,
            pa.Check.isin(['Profissional', 'Público Geral', 'Qualificado']),
            nullable=True,
            coerce=True,
        ),
        'c299': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c307': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c308': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c309': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c310': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c311': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c4234': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c325': pa.Column(
            float,
            nullable=True,
            # coerce=True,
        ),
        'c326': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c4236': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'a643': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c327': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c328': pa.Column(
            str,
            pa.Check.isin(['Aberto', 'Fechado']),
            nullable=True,
            coerce=True,
        ),
        'c329': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c330': pa.Column(
            str,
            pa.Check.isin(['Público Geral', 'Profissional', 'Qualificado']),
            nullable=True,
            coerce=True,
        ),
        'c313': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c314': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'c4142': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c315': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c316': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'a642': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c317': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c318': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c319': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c320': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c321': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c322': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c323': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c324': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c5113': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c5112': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c4137': pa.Column(
            str,
            pa.Check.isin(['PJ', 'PF']),
            nullable=True,
            coerce=True,
        ),
        'c5116': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c5117': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
    }
)


fundo_classe_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(10235), nullable=False, coerce=True
        ),
        'SERIE': pa.Column(str, nullable=False, coerce=True),
        # 'DATASER': pa.Column(pa.DateTime, nullable=False, coerce=True),
        'INTERVALO': pa.Column(
            int, pa.Check.equal_to(1), nullable=False, coerce=True
        ),
        'FAMCOMPL': pa.Column(
            str,
            pa.Check.equal_to('Geral'),
            nullable=False,
            coerce=True,
        ),
        'c312': pa.Column(
            int,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'c4145': pa.Column(
            int,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'c264': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'c265': pa.Column(
            int,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'c4157': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c266': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c4146': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=False,
            coerce=True,
        ),
        'c277': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'a618': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'c289': pa.Column(
            str,
            pa.Check.isin(['Cancelado', 'Em Funcionamento Normal', 'Em Liquidação',
            'Fase Pré-Operacional', 'Em Análise']),
            nullable=False,
            coerce=True,
        ),
        'c290': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c291': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c5107': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c292': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c293': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c294': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c295': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c4152': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c296': pa.Column(
            str,
            pa.Check.isin(['Aberto', 'Fechado']),
            nullable=True,
            coerce=True,
        ),
        'c297': pa.Column(
            str,
            pa.Check.isin(['S', 'N']),
            nullable=True,
            coerce=True,
        ),
        'c298': pa.Column(
            str,
            pa.Check.isin(['Profissional', 'Público Geral', 'Qualificado']),
            nullable=True,
            coerce=True,
        ),
        'c299': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c307': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c308': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c309': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c310': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c311': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c313': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c314': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'c4142': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c315': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c316': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'a642': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c317': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c318': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c319': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c320': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c321': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c322': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c323': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c324': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c5113': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c5112': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c4137': pa.Column(
            str,
            pa.Check.isin(['PJ', 'PF']),
            nullable=True,
            coerce=True,
        ),
        'c5116': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c5117': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
    }
)

fundo_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(10235), nullable=False, coerce=True
        ),
        'SERIE': pa.Column(str, nullable=False, coerce=True),
        # 'DATASER': pa.Column(pa.DateTime, nullable=False, coerce=True),
        'INTERVALO': pa.Column(
            int, pa.Check.equal_to(1), nullable=False, coerce=True
        ),
        'FAMCOMPL': pa.Column(
            str,
            pa.Check.equal_to('Geral'),
            nullable=False,
            coerce=True,
        ),
        'c312': pa.Column(
            int,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'c313': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c314': pa.Column(
            float,
            nullable=True,
            coerce=True,
        ),
        'c4142': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c315': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c316': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'a642': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c317': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c318': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c319': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c320': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c321': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c322': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c323': pa.Column(
            str,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'c324': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c5113': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c5112': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c4137': pa.Column(
            str,
            pa.Check.isin(['PJ', 'PF']),
            nullable=True,
            coerce=True,
        ),
        'c5116': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        'c5117': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
    }
)

diario_schema = pa.DataFrameSchema(
    {
        'ID': pa.Column(
            int, pa.Check.equal_to(10235), nullable=False, coerce=True
        ),
        'SERIE': pa.Column(str, nullable=False, coerce=True),
        # 'DATASER': pa.Column(pa.DateTime, nullable=False, coerce=True),
        'INTERVALO': pa.Column(
            int, pa.Check.equal_to(1), nullable=False, coerce=True
        ),
        'FAMCOMPL': pa.Column(
            str,
            pa.Check.equal_to('Geral'),
            nullable=False,
            coerce=True,
        ),
        'a68': pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        'a8': pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        'a20': pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        'a314': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'a279': pa.Column(
            float,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'a281': pa.Column(
            int,
            pa.Check.greater_than_or_equal_to(0),
            nullable=False,
            coerce=True,
        ),
        'a84': pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
    }
)