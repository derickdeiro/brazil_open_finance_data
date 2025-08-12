import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format

fre_posicao_acionaria_schema = pa.DataFrameSchema(
    {
        'NumeroIdentificadorAcionista': pa.Column(
            pa.Int, 
            nullable=False,
            coerce=True
        ),
        'NumeroIdentificadorAcionistaPai': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'CpfCnpjAcionista': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'TipoPessoaAcionista': pa.Column(
            pa.String,
            pa.Check.isin(['AcoesTesouraria', 'Fisica', 'Juridica', 'Outros', 'Total']),
            nullable=False,
            coerce=True
        ),
        'UltimaAlteracao': pa.Column(
            pa.String,
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="UltimaAlteracao diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=True,
            coerce=True
        ),
        'QtdAcoesOrdinaria': pa.Column(
            pa.Int,
            pa.Check.ge(0),
            nullable=False,
            coerce=True
        ),
        'AcoesOrdinariasPorcent': pa.Column(
            pa.Float,
            pa.Check.ge(0),
            pa.Check.le(100),
            nullable=False,
            coerce=True
        ),
        'QtdAcoesPreferenciais': pa.Column(
            pa.Int,
            pa.Check.ge(0),
            nullable=False,
            coerce=True
        ),
        'AcoesPreferenciaisPorcent': pa.Column(
            pa.Float,
            pa.Check.ge(0),
            pa.Check.le(100),
            nullable=False,
            coerce=True
        ),
        'QtdTotalAcoes': pa.Column(
            pa.Int,
            pa.Check.ge(0),
            nullable=False,
            coerce=True
        ),
        'TotalAcoesPorcent': pa.Column(
            pa.Float,
            pa.Check.ge(0),
            pa.Check.le(100),
            nullable=False,
            coerce=True
        ),
        'NomeAcionista': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'Nacionalidade': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'UF': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'ParticipaAcordosAcionistas': pa.Column(
            pa.String,
            pa.Check.isin(['Sim', 'Nao', 'Não']),
            nullable=True,
            coerce=True
        ),
        'AcionistaControlador': pa.Column(
            pa.String,
            pa.Check.isin(['Sim', 'Nao', 'Não']),
            nullable=True,
            coerce=True
        ),
        'AcionistaResidenteExterior': pa.Column(
            pa.String,
            pa.Check.isin(['Sim', 'Nao', 'Não']),
            nullable=True,
            coerce=True
        ),
        'PassaporteAcionista': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'NomeRepresentante': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'PessoaFisicaOuJuridica': pa.Column(
            pa.String,
            pa.Check.isin(['Fisica', 'Juridica']),
            nullable=True,
            coerce=True
        ),
        'CpfCnpj': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
    }
)
