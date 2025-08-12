import pandera.pandas as pa

fre_remun_min_med_max_schema = pa.DataFrameSchema(
    {
        'ExercicioSocial': pa.Column(
            pa.Date,
            nullable=False,
            coerce=True
        ),
        'OrgaoAdm': pa.Column(
            pa.String,
            nullable=False,
            coerce=True
        ),
        'NumTotalMembros': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'NumMembrosRemun': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'MaiorRemun': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'MenorRemun': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'MediaRemun': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'Observacao': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
    }
)

fre_remun_orgao_schema = pa.DataFrameSchema(
    {
        'ExercicioSocial': pa.Column(
            pa.Date,
            nullable=False,
            coerce=True
        ),
        'OrgaoAdm': pa.Column(
            pa.String,
            nullable=False,
            coerce=True
        ),
        'NumTotalMembros': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'NumMembrosRemun': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'ValorTotalRemunOrgao': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'Esclarecimento': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'Salário': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'Beneficios': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'ParticipacaoComites': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'DescricaoOutrasRemun': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'Bonus': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'ParticipacaoResultados': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'ParticipacaoReunioes': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'Comissoes': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'DescricaoOutrasRemunVariaveis': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'PosEmprego': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'CessacaoCargo': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'BaseadoAcoes': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'Observacao': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'Outros': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
    }
)

fre_remun_variavel_schema = pa.DataFrameSchema(
    {
        'ExercicioSocial': pa.Column(
            pa.Date,
            nullable=False,
            coerce=True
        ),
        'OrgaoAdm': pa.Column(
            pa.String,
            nullable=False,
            coerce=True
        ),
        'NumTotalMembros': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'NumMembrosRemun': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'BonusValorMinimoPrevistoPlanoRemuneracao': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'BonusValorMaximoPrevistoPlanoRemuneracao': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'BonusValorPrevistoPlanoRemuneracaoMetaAtingida': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'BonusValorReconhecidoUltimo3ExerciciosSociais': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'ParticipacaoResultadoValorMinimoPrevistoPlanoRemuneracao': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'ParticipacaoResultadoValorMaximoPrevistoPlanoRemuneracao': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'ParticipacaoResultadoValorPrevistoPlanoRemuneracaoMetaAtingida': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'ParticipacaoResultadoValorReconhecidoUltimo3ExerciciosSociais': pa.Column(
            pa.Float,
            nullable=False,
            coerce=True
        ),
        'Esclarecimento': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
    }
)

