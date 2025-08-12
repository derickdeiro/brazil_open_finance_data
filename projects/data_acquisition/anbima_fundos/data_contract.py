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

anbima_fundos_schema = pa.DataFrameSchema(
    {
        'codigo_classe': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'tipo_identificador_classe': pa.Column(
            str,
            pa.Check.equal_to('CNPJ'),
            nullable=False,
            coerce=True,
        ),
        'identificador_classe': pa.Column(
            int,
            nullable=False,
            coerce=True,
        ),
        'razao_social_classe': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'nome_comercial_classe': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'data_inicio_atividade_classe': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=False,
            coerce=True,
        ),
        'data_encerramento_classe': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),

        'subclasses': pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),

        'nivel1_categoria_classe': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'data_vigencia_classe': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=False,
            coerce=True,
        ),
        'data_atualizacao_classe': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=False,
            coerce=True,
        ),
        'codigo_fundo': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'tipo_identificador_fundo': pa.Column(
            str,
            pa.Check.equal_to('CNPJ'),
            nullable=False,
            coerce=True,
        ),
        'identificador_fundo': pa.Column(
            int,
            nullable=False,
            coerce=True,
        ),
        'razao_social_fundo': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'nome_comercial_fundo': pa.Column(
            str,
            nullable=False,
            coerce=True,
        ),
        'tipo_fundo': pa.Column(
            str,
            pa.Check.equal_to('FIF'),
            nullable=False,
            coerce=True,
        ),
        'data_encerramento_fundo': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=True,
            coerce=True,
        ),
        'data_vigencia_fundo': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=False,
            coerce=True,
        ),
        'data_atualizacao_fundo': pa.Column(
            pa.String,
            pa.Check(lambda s: is_valid_date_format(s)),
            nullable=False,
            coerce=True,
        )
    }
)
